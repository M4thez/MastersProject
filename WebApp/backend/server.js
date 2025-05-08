const express = require('express');
const cors = require('cors');
require('dotenv').config();
const opensearchClient = require('./opensearchClient'); // Import the client

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors()); // Enable CORS for all routes (adjust for production)
app.use(express.json()); // To parse JSON request bodies

// Search endpoint
// Post request to search for a query in the OpenSearch index 
// not get because we are sending a body with the query
app.post('/api/search', async (req, res) => {
    const { queryText, filters = {}, page = 1, pageSize = 10, sortBy } = req.body;
    const from = (page - 1) * pageSize; // Calculate the starting point for pagination
    console.log('Search request body:', req.body);

    // --- If queryText is empty AND no filters are applied, return no results immediately ---
    if ((!queryText || queryText.trim() === "") && Object.keys(filters).length === 0) {
        console.log('Empty query text and no filters. Returning no results.');
        return res.json({
            hits: [],
            total: 0,
            aggregations: {} // Or fetch initial aggregations with a separate match_all query if needed
        });
    }


    // Construct the search query
    const osQueryBody = {
        from: from,
        size: pageSize,
        query: {
            bool: {
                must: [],
                filter: []
            }
        },
        aggs: {
            papers_by_year: {
                terms: {
                    field: "publication_date",
                    size: 20 // Adjust the size as needed
                }
            },
            papers_by_type: {
                terms: {
                    field: "type",
                    size: 10 // Adjust the size as needed
                }
            },
            papers_by_university: {
                terms: {
                    field: "university_key",
                    size: 10 // Adjust the size as needed
                }
            }
        }
    }

    // 1. Handle free-text query (queryText)
    if (queryText && queryText.trim() !== "") {
        osQueryBody.query.bool.must.push({
            multi_match: {
                query: queryText,
                fields: ["title^3", "abstract", "authors^2", "keywords.display_name^1.5"],
                fuzziness: "AUTO" // Allow some typos
            }
        });
    } else {
        // If queryText is empty BUT filters might be present,
        // match based on filters.
        if (Object.keys(filters).length > 0) {
            osQueryBody.query.bool.must.push({ match_all: {} });
        }
    }

    // 2. Handle filters
    if (filters.publication_year) {
        osQueryBody.query.bool.filter.push({
            term: { publication_year: filters.publication_year }
        });
    }
    if (filters.type) {
        osQueryBody.query.bool.filter.push({
            term: { type: filters.type }
        });
    }
    if (filters.university) {
        osQueryBody.query.bool.filter.push({
            term: { "university_key.keyword": filters.university }
        });
    }

    // 3. Handle Sorting
    if (sortBy) {
        osQueryBody.sort = [{
            [sortBy.field]: {
                order: sortBy.order, // "asc" or "desc"

            }
        }];
    } else if (!sortBy && !queryText && queryText.trim() !== "") {
        osQueryBody.sort = [{
            "publication_year": {
                order: "desc" // Default sorting by publication year descending
            }
        }];
    }

    console.log('Constructed OpenSearch Query:', JSON.stringify(osQueryBody, null, 2));



    try {
        const result = await opensearchClient.search({
            index: process.env.OPENSEARCH_INDEX,
            body: osQueryBody
        });

        const responseData = {
            hits: result.body.hits.hits.map(hit => ({ id: hit._id, ...hit._source, score: hit._score })),
            total: result.body.hits.total.value,
            aggregations: result.body.aggregations
        };
        res.json(responseData);
    } catch (error) {
        console.error('Error during OpenSearch query:', error.meta ? error.meta.body : error);
        res.status(500).json({ message: 'Error querying OpenSearch', error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`Node.js backend server listening on port ${PORT}`);
})