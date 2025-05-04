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
PARQUET_FILE_PATH = 'papers_clean.parquet'
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
