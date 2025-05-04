import os
import sys
from dotenv import load_dotenv
from opensearchpy import OpenSearch

# Load environment variables from .env file
load_dotenv()

# Configuration
host = 'localhost'
port = 9200

OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASSWORD = os.getenv('OPENSEARCH_PASSWORD')
if OPENSEARCH_USER and OPENSEARCH_PASSWORD:
    auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
else:
    auth = None

# Create the client with SSL/TLS and hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

print(client.info())
