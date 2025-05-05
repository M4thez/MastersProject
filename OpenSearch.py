import os
import sys
import logging
import pandas as pd
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers, exceptions
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
PARQUET_FILE_PATH = 'papers_clean2.parquet'
INDEX_NAME = 'university_papers'

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),  # Log to a file
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


def define_opensearch_mapping():
    """Defines the OpenSearch index mapping."""

    return {
        "properties": {
            "university_key": {
                "type": "keyword",
                "meta": {"description": "Internal key representing the university this record was queried for (e.g., GR_UOP)."}
            },
            "openalex_id": {
                "type": "keyword",
                "meta": {"description": "Unique OpenAlex Work ID (URL)."}
            },
            "doi": {
                "type": "keyword",
                "meta": {"description": "Digital Object Identifier (URL/string)."}
            },
            "language": {
                "type": "keyword",
                "meta": {"description": "Language code (e.g., 'en'). Useful for filtering."}
            },
            "type": {
                "type": "keyword",
                "meta": {"description": "OpenAlex work type (e.g., 'article', 'book'). Useful for filtering/aggregation."}
            },
            "title": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 512
                    }
                },
                "meta": {"description": "Work title. Indexed for full-text search ('title') and exact match/sorting/aggregation ('title.keyword')."}
            },
            "publication_date": {
                "type": "date",
                "format": "yyyy-MM-dd",
                "meta": {"description": "Full publication date. Format must match incoming data (YYYY-MM-DD)."}
            },
            "publication_year": {
                "type": "integer",
                "meta": {"description": "Year of publication. Useful for range filtering and aggregation."}
            },
            "open_access": {
                "properties": {
                    "any_repository_has_fulltext": {
                        "type": "boolean"
                    },
                    "is_oa": {
                        "type": "boolean"
                    },
                    "oa_status": {
                        "type": "keyword"
                    },
                    "oa_url": {
                        "type": "keyword",
                        "index": False
                    }
                },
                "meta": {"description": "Object containing Open Access status details. URL stored but not indexed."}
            },
            "institutions": {
                "type": "keyword",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                },
                "meta": {"description": "List of institution display names associated with the work. Useful for filtering/aggregation."}
            },
            "authors": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                },
                "meta": {"description": "List of author names. Indexed for text search ('authors') and exact match/aggregation ('authors.keyword')."}
            },
            "cited_by_count": {
                "type": "integer",
                "meta": {"description": "Number of citations received according to OpenAlex."}
            },
            "fwci": {
                "type": "float",
                "meta": {"description": "Field-Weighted Citation Impact."}
            },
            "citation_normalized_percentile": {
                "properties": {
                    "is_in_top_10_percent": {"type": "boolean"},
                    "is_in_top_1_percent": {"type": "boolean"},
                    "value": {"type": "float"}
                },
                "meta": {"description": "Citation percentile details."}
            },
            "abstract": {
                "type": "text",
                "analyzer": "standard",
                "meta": {"description": "Work abstract, indexed for full-text search."}
            },
            "primary_topic": {
                "properties": {
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "id": {"type": "keyword"},
                    "score": {"type": "float"},
                    "field": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "subfield": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "domain": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    }
                },
                "meta": {"description": "The single most relevant topic assigned by OpenAlex, including its hierarchy and score."}
            },
            "topics": {
                "type": "nested",
                "properties": {
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "id": {"type": "keyword"},
                    "score": {"type": "float"},
                    "field": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "subfield": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "domain": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    }
                },
                "meta": {"description": "List of all topics assigned by OpenAlex. 'nested' type allows querying fields within a single topic object."}
            },
            "keywords": {
                "type": "nested",
                "properties": {
                    "display_name": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "meta": {"description": "The keyword text itself. Indexed for text search ('keywords.display_name') and exact match/aggregation ('keywords.display_name.keyword')."}
                    },
                    "id": {
                        "type": "keyword",
                    },
                    "score": {
                        "type": "float",
                    }
                },
                "meta": {"description": "List of author-supplied keywords, each with display name, ID, and score. 'nested' allows querying fields within a single keyword object."}
            },


            "cited_by_api_url": {
                "type": "keyword",
                "index": False,
                "meta": {"description": "OpenAlex API URL to retrieve citing works. Stored but not indexed."}
            },
            "updated_date": {
                "type": "date",
                "format": "basic_date_time",
                "meta": {"description": "Timestamp when the OpenAlex record was last updated."}
            },
            "created_date": {
                "type": "date",
                "format": "yyyy-MM-dd",
                "meta": {"description": "Date when the OpenAlex record was created."}
            }
        }
    }

# Main Pipeline Execution


def main():
    """Runs the entire OpenSearch pipeline."""
    logging.info("--- Starting Pipeline ---")

    # --- Stage 1: Connect to OpenSearch ---
    opensearch_client = create_opensearch_client()
    if not opensearch_client:
        logging.error("Failed to connect to OpenSearch. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
