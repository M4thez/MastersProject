const { Client } = require('@opensearch-project/opensearch');
require('dotenv').config();

const user = process.env.OPENSEARCH_USER;
const rawPassword = process.env.OPENSEARCH_PASSWORD;

if (!user || !rawPassword) {
    console.error("OpenSearch user or password not set in environment variables!");
    process.exit(1);
}

const encodedPassword = encodeURIComponent(rawPassword);

const client = new Client({
    node: `${process.env.OPENSEARCH_SCHEME}://${user}:${encodedPassword}@${process.env.OPENSEARCH_HOST}:${process.env.OPENSEARCH_PORT}`
});

client.ping()
    .then(response => console.log('Node.js: Successfully connected to OpenSearch!'))
    .catch(error => console.error('Node.js: OpenSearch connection error:', error));

module.exports = client;