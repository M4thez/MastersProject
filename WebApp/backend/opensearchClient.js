const { Client } = require('@opensearch-project/opensearch');
require('dotenv').config();

const client = new Client({
    node: `${process.env.OPENSEARCH_SCHEME}://${process.env.OPENSEARCH_USER}:${process.env.OPENSEARCH_PASSWORD}@${process.env.OPENSEARCH_HOST}:${process.env.OPENSEARCH_PORT}`
});

client.ping()
    .then(response => console.log('Node.js: Successfully connected to OpenSearch!'))
    .catch(error => console.error('Node.js: OpenSearch connection error:', error));

module.exports = client;