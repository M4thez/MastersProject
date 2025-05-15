import os
import sys
import logging
import pandas as pd
from opensearchpy import OpenSearch, helpers, exceptions
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration for OpenSearch
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASSWORD = os.getenv('OPENSEARCH_PASSWORD')
OPENSEARCH_SCHEME = 'https'
OPENSEARCH_PORT = 9200
OPENSEARCH_HOST = 'localhost'

# FILE/INDEX Names
PARQUET_FILE_PATH = 'authors_raw_data.parquet'
INDEX_NAME = 'university_authors'

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_authors.log"),  # Log to a file
        logging.StreamHandler(sys.stdout)    # Log to console
    ]
)


def create_opensearch_client():
    """Creates and configures the OpenSearch client."""

    logging.info(
        f"Attempting to connect to OpenSearch: {OPENSEARCH_SCHEME}://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    http_auth = None

    # --- Authentication Setup ---
    if OPENSEARCH_USER and OPENSEARCH_PASSWORD:
        http_auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
        logging.info(
            f"Using Basic Authentication for user '{OPENSEARCH_USER}'.")
    else:
        logging.warning(
            "OpenSearch username/password not set. Connecting without authentication.")

    # SSL Settings (adjust verify_certs for production)
    use_ssl = (OPENSEARCH_SCHEME == 'https')
    if use_ssl:
        logging.info("Using SSL for OpenSearch connection.")
    else:
        logging.warning("Not using SSL for OpenSearch connection.")

    try:
        client = OpenSearch(
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
            http_auth=http_auth,
            http_compress=True,
            use_ssl=use_ssl,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )
        # Verify connection
        if not client.ping():
            raise ConnectionError("OpenSearch ping failed.")
        logging.info("Successfully connected to OpenSearch.")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to OpenSearch: {e}", exc_info=True)
        return None


def define_authors_mapping():
    """Defines the OpenSearch index mapping."""

    return {
        "properties": {
            "university_key": {
                "type": "keyword"
            },
            "id": {
                "type": "keyword"
            },
            "orcid": {
                "type": "keyword"
            },
            "display_name": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "display_name_alternatives": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "works_count": {
                "type": "integer"
            },
            "cited_by_count": {
                "type": "integer"
            },
            "ids": {
                "properties": {
                    "openalex": {"type": "keyword"},
                    "orcid": {"type": "keyword"},
                    "scopus": {"type": "keyword"},
                    "twitter": {"type": "keyword"}
                }
            },
            "last_known_institutions": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "ror": {"type": "keyword"},
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "country_code": {"type": "keyword"},
                    "type": {"type": "keyword"}
                }
            },
            "affiliations": {
                "type": "nested",
                "properties": {
                    "years": {"type": "integer"},
                    "institution": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "ror": {"type": "keyword"},
                            "display_name": {
                                "type": "text",
                                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                            },
                            "country_code": {"type": "keyword"},
                            "type": {"type": "keyword"}
                        }
                    }
                }
            },
            "summary_stats": {
                "properties": {
                    "2yr_mean_citedness": {"type": "float"},
                    "h_index": {"type": "integer"},
                    "i10_index": {"type": "integer"}
                }
            },
            "counts_by_year": {
                "type": "nested",
                "properties": {
                    "year": {"type": "integer"},
                    "works_count": {"type": "integer"},
                    "cited_by_count": {"type": "integer"}
                }
            },
            "x_concepts": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "wikidata": {"type": "keyword"},
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "level": {"type": "integer"},
                    "score": {"type": "float"}
                }
            },
            "created_date": {
                "type": "date",
                "format": "yyyy-MM-dd"
            },
            "updated_date": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            }
        }
    }


def create_opensearch_index(client, index_name, mapping):
    """Creates the OpenSearch index with the specified mapping if it doesn't exist."""

    try:
        if not client.indices.exists(index=index_name):
            logging.info(f"Index '{index_name}' does not exist. Creating it.")
            response = client.indices.create(
                index=index_name, body={'mappings': mapping})
            logging.info(
                f"Index '{index_name}' created successfully: {response}")
            return True
        else:
            logging.info(
                f"Index '{index_name}' already exists. Skipping creation.")
            return True
    except exceptions.RequestError as re:
        logging.error(
            # exception tuple info for more details
            f"Error creating index '{index_name}'. Details: {re.info['error']['root_cause']}", exc_info=True)
        return False
    except exceptions.ConnectionError as ce:
        logging.error(
            f"Connection error while creating index '{index_name}': {ce}", exc_info=True)
        return False
    except Exception as e:
        logging.error(
            f"Unexpected error while creating index '{index_name}': {e}", exc_info=True)
        return False


def load_from_parquet(filename):
    """Loads data from a Parquet file into a DataFrame."""
    logging.info(f"Loading data from Parquet file: {filename}")

    try:
        df = pd.read_parquet(filename, engine='pyarrow')
        logging.info(f"Data loaded successfully from {filename}.")
        df = df.astype(object).where(
            pd.notnull(df), None)  # Convert NaN to None
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {filename}", exc_info=True)
        return None
    except pd.errors.EmptyDataError:
        logging.error(f"Empty data in file: {filename}", exc_info=True)
        return None
    except Exception as e:
        logging.error(
            f"Error loading data from file: {filename}. Error: {e}", exc_info=True)
        return None


def generate_bulk_actions(dataframe, target_index, id_col):
    """Generator function to yield bulk API actions from DataFrame rows."""
    if id_col not in dataframe.columns:
        logging.error(
            f"ID column '{id_col}' not found in DataFrame. Cannot generate bulk actions.")
        return

    skipped_count = 0
    records = dataframe.to_dict(orient='records')
    logging.info(f"Generating bulk actions for {len(records)} records...")

    for doc in records:
        doc_id = doc.get(id_col)

        if doc_id is None:
            skipped_count += 1
            continue

        yield {
            "_index": target_index,
            "_id": str(doc_id),  # Ensure ID is a string
            "_source": doc      # Use the potentially cleaned doc
        }
    if skipped_count > 0:
        logging.warning(
            f"Skipped {skipped_count} records because '{id_col}' was missing or null.")


def index_data_to_opensearch(client, dataframe, index_name):
    """Indexes data from a DataFrame into OpenSearch using streaming bulk."""
    if dataframe is None or dataframe.empty:
        logging.error("DataFrame is empty or None. Cannot index data.")
        return False
    logging.info(
        f"Starting bulk indexing of {len(dataframe)} records to '{index_name}'...")
    success_count = 0
    failed_count = 0
    total_processed = 0

    try:
        for ok, action_info in helpers.streaming_bulk(
            client=client,
            actions=generate_bulk_actions(dataframe, index_name, "id"),
            chunk_size=500,
            max_retries=4,
            initial_backoff=1,
            max_backoff=5,
            request_timeout=60,
            raise_on_error=False
        ):
            total_processed += 1
            if ok:
                success_count += 1
            else:
                failed_count += 1
                logging.error(f"Failed to index document: {action_info}")

            if total_processed % 1000 == 0:
                logging.info(
                    f"Processed {total_processed} records. Success: {success_count}, Failed: {failed_count}")

        logging.info("Bulk indexing finished.")
        logging.info(f"  Total actions attempted: {total_processed}")
        logging.info(f"  Successfully indexed: {success_count}")
        logging.info(f"  Failed operations: {failed_count}")

        # Optional: Refresh the index only if everything succeeded
        if success_count > 0 and failed_count == 0:
            try:
                client.indices.refresh(index=index_name)
                logging.info(f"Index '{index_name}' refreshed.")
            except Exception as e:
                logging.error(f"Error refreshing index '{index_name}': {e}")
        elif failed_count > 0:
            logging.warning("Index not refreshed due to indexing errors.")

        return failed_count == 0  # Return True if successful

    except exceptions.ApiError as ae:
        logging.error(
            f"An OpenSearch API error occurred during bulk indexing: {ae}", exc_info=True)
        return False
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during bulk indexing: {e}", exc_info=True)
        return False


def main():
    """Runs the entire OpenSearch pipeline."""
    logging.info("--- Starting Pipeline ---")

    # --- Stage 1: Connect to OpenSearch ---
    opensearch_client = create_opensearch_client()
    if not opensearch_client:
        logging.error("Failed to connect to OpenSearch. Exiting.")
        sys.exit(1)

    # --- Stage 2: Define Index Mapping and Create Index ---
    mapping = define_authors_mapping()
    if not create_opensearch_index(opensearch_client, INDEX_NAME, mapping):
        logging.error("Failed to create OpenSearch index. Exiting.")
        sys.exit(1)

    # --- Stage 3: Load Data from Parquet File ---
    dataframe_to_index = load_from_parquet(PARQUET_FILE_PATH)
    if dataframe_to_index is None or dataframe_to_index.empty:
        logging.error(
            f"Failed to load data from {PARQUET_FILE_PATH}. Exiting.")
        sys.exit(1)

    # --- Stage 4: Index Data to OpenSearch ---
    indexing_success = index_data_to_opensearch(
        opensearch_client, dataframe_to_index, INDEX_NAME)
    if indexing_success:
        logging.info("^^^ Data indexed successfully. ^^^")
    else:
        logging.error("--- --- --- Pipeline completed with indexing errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()
