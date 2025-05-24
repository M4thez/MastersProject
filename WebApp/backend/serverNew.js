const express = require('express');
const cors = require('cors');
require('dotenv').config();
const opensearchClient = require('./opensearchClient');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// --- Generic Search Helper Function ---
async function executeOpenSearchQuery(targetIndex, queryText, filters, page, pageSize, sortBy, defaultSortField, defaultSortOrder = "desc") {
    const from = (page - 1) * pageSize;
    console.log(`Executing search on index '${targetIndex}':`, { queryText, filters, page, pageSize, sortBy });
    console.log('AAA', targetIndex);
    console.log('BBB',process.env.OPENSEARCH_PAPERS_INDEX,process.env.OPENSEARCH_AUTHORS_INDEX,process.env.OPENSEARCH_PROJECTS_INDEX);
    const hasQueryText = queryText && queryText.trim() !== "";
    const hasFilters = Object.keys(filters).length > 0;

    const osQueryBody = {
        from: from,
        size: pageSize,
        query: {
            bool: {
                must: [],
                filter: []
            }
        },
        // Aggregations will be added specifically for each type
    };

    // 1. Main Query Part
    if (hasQueryText) {
        let searchFields = [];
        if (targetIndex === process.env.OPENSEARCH_PAPERS_INDEX) {
            searchFields = ["title^3", "abstract", "authors^2", "keywords.display_name^1.5"];
        } else if (targetIndex === process.env.OPENSEARCH_AUTHORS_INDEX) {
            searchFields = ["display_name^3", "display_name_alternatives^2", "x_concepts.display_name^1.5", "last_known_institutions.display_name"];
        } else if (targetIndex === process.env.OPENSEARCH_PROJECTS_INDEX) {
            searchFields = ["title^3", "summary^2", "acronym^1.5", "subjects^1.2"];
        }
        if (searchFields.length > 0) {
            osQueryBody.query.bool.must.push({
                multi_match: { query: queryText, fields: searchFields, fuzziness: "AUTO" }
            });
        } else { // Fallback if no specific fields for an unknown index (shouldn't happen with defined endpoints)
            console.log('FALLABACK CASE');
            osQueryBody.query.bool.must.push({ simple_query_string: { query: queryText, default_operator: "AND" } });
        }
    } else {
        osQueryBody.query.bool.must.push({ match_all: {} });
        if (!hasFilters && (targetIndex === process.env.OPENSEARCH_PAPERS_INDEX || targetIndex === process.env.OPENSEARCH_PROJECTS_INDEX || targetIndex === process.env.OPENSEARCH_AUTHORS_INDEX)) {
            // For initial load of papers/projects/authors, fetch only aggregations
            osQueryBody.size = 0;
        }
    }

    // 2. Aggregations (specific to index type)
    if (targetIndex === process.env.OPENSEARCH_PAPERS_INDEX) {
        osQueryBody.aggs = {
            papers_by_date: { terms: { field: "publication_date", size: 20 } },
            papers_by_type: { terms: { field: "type", size: 20 } },
            papers_by_university: { terms: { field: "university_key", size: 20 } }
        };
    } else if (targetIndex === process.env.OPENSEARCH_AUTHORS_INDEX) {
        osQueryBody.aggs = {
            authors_by_university: { terms: { field: "university_key", size: 20 } },
            authors_by_lki_type: {
                nested: { path: "last_known_institutions" },
                aggs: { types: { terms: { field: "last_known_institutions.type", size: 10 } } }
            }
        };
    } else if (targetIndex === process.env.OPENSEARCH_PROJECTS_INDEX) {
        // osQueryBody.aggs = {
        //     projects_by_funder: {
        //         nested: { path: "fundings" },
        //         aggs: { funders: { terms: { field: "fundings.name", size: 10 } } }
        //     },
        //     projects_by_year: { terms: { field: "startDate", format: "yyyy", size: 10 } }

        // };
    }


    // 3. Filters (specific to index type)
    if (targetIndex === process.env.OPENSEARCH_PAPERS_INDEX) {
        if (filters.publication_year) osQueryBody.query.bool.filter.push({ term: { publication_year: filters.publication_date } });
        if (filters.type) osQueryBody.query.bool.filter.push({ term: { type: filters.type } });
        if (filters.university_key) osQueryBody.query.bool.filter.push({ term: { "university_key": filters.university_key } }); 
    } 
    else if (targetIndex === process.env.OPENSEARCH_AUTHORS_INDEX) {
        if (filters.university_key) osQueryBody.query.bool.filter.push({ term: { "university_key": filters.university_key } });
        if (filters.lki_type) {
            osQueryBody.query.bool.filter.push({
                nested: {
                    path: "last_known_institutions",
                    query: { term: { "last_known_institutions.type": filters.lki_type } }
                }
            });
        }
    }
     else if (targetIndex === process.env.OPENSEARCH_PROJECTS_INDEX) {
        if (filters.funder_name) { 
            osQueryBody.query.bool.filter.push({
                nested: {
                    path: "fundings",
                    query: { term: { "fundings.name": filters.funder_name } }
                }
            });
        }
        if (filters.startYear) { // Example filter for project start year
            osQueryBody.query.bool.filter.push({
                range: { "startDate": { gte: `${filters.startYear}-01-01`, lte: `${filters.startYear}-12-31`, format: "yyyy-MM-dd" } }
            });
        }
    }


    // 4. Sorting
    if (sortBy) {
        osQueryBody.sort = [{ [sortBy.field]: { order: sortBy.order } }];
    } else if (!hasQueryText) { // Default sort if no query text
        osQueryBody.sort = [{ [defaultSortField]: { order: defaultSortOrder } }];
    }
    // If hasQueryText and no sortBy, OpenSearch sorts by _score by default

    console.log(`Constructed OpenSearch Query for ${targetIndex}:`, JSON.stringify(osQueryBody, null, 2));

    try {
        const result = await opensearchClient.search({
            index: targetIndex,
            body: osQueryBody
        });
        return {
            hits: result.body.hits.hits.map(hit => ({ id: hit._id, ...hit._source, score: hit._score })),
            total: result.body.hits.total.value,
            aggregations: result.body.aggregations || {}
        };
    } catch (error) {
        console.error(`Error in executeOpenSearchQuery for ${targetIndex}:`, error.meta ? error.meta.body : error);
        // Re-throw a more generic error or a structured error for the route handler
        throw new Error(`OpenSearch query failed for index ${targetIndex}: ${error.message}`);
    }
}


// --- Papers Search Endpoint ---
app.post('/api/search/papers', async (req, res) => {
    const { queryText, filters = {}, page = 1, pageSize = 10, sortBy = null } = req.body;
    try {

        console.log('aaa',process.env.OPENSEARCH_PAPERS_INDEX);
        const responseData = await executeOpenSearchQuery(
            process.env.OPENSEARCH_PAPERS_INDEX,
            queryText, filters, page, pageSize, sortBy,
            "publication_date", "desc" // Default sort field and order for papers
        );
        res.json(responseData);
    } catch (error) {
        console.error(`Papers Search Endpoint Error:`, error);
        res.status(500).json({ message: 'Error querying for papers', error: error.message });
    }
});

// --- Authors Search Endpoint ---
app.post('/api/search/authors', async (req, res) => {
    const { queryText, filters = {}, page = 1, pageSize = 10, sortBy = null } = req.body;
    try {
        const responseData = await executeOpenSearchQuery(
            process.env.OPENSEARCH_AUTHORS_INDEX,
            queryText, filters, page, pageSize, sortBy,
            "display_name", "asc" // Default sort field and order for authors
        );
        res.json(responseData);
    } catch (error) {
        console.error(`Authors Search Endpoint Error:`, error);
        res.status(500).json({ message: 'Error querying for authors', error: error.message });
    }
});

// --- Projects Search Endpoint ---
app.post('/api/search/projects', async (req, res) => {
    const { queryText, filters = {}, page = 1, pageSize = 10, sortBy = null } = req.body;
    try {
        const responseData = await executeOpenSearchQuery(
            process.env.OPENSEARCH_PROJECTS_INDEX,
            queryText, filters, page, pageSize, sortBy,
            "startDate", "desc" // Default sort field and order for projects
        );
        res.json(responseData);
    } catch (error) {
        console.error(`Projects Search Endpoint Error:`, error);
        res.status(500).json({ message: 'Error querying for projects', error: error.message });
    }
});


app.listen(PORT, () => {
    console.log(`Node.js backend server listening on port ${PORT}`);
});