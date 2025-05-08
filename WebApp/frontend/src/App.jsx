import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import "./App.css";
import "./variables.css";

const BACKEND_URL = "http://82.145.73.10:3001/api/search"; // Node.js backend URL

function App() {
  const [queryText, setQueryText] = useState("");
  const [results, setResults] = useState([]);
  const [totalHits, setTotalHits] = useState(0);
  const [aggregations, setAggregations] = useState({});
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // --- Filters State ---
  const [selectedYear, setSelectedYear] = useState("");
  const [selectedType, setSelectedType] = useState("");
  const [selectedUniversity, setSelectedUniversity] = useState("");

  const performSearch = useCallback(
    async (pageToFetch = 1) => {
      setLoading(true);
      setError(null);
      setCurrentPage(pageToFetch);

      const filters = {};
      if (selectedYear) filters.publication_year = selectedYear;
      if (selectedType) filters.type = selectedType;
      if (selectedUniversity) filters.university = selectedUniversity;
      // Add more filters here

      try {
        const response = await axios.post(BACKEND_URL, {
          queryText: queryText,
          filters: filters,
          page: pageToFetch,
          pageSize: pageSize,
          // sortBy: { field: "publication_date", order: "desc" } // Example sort
        });
        console.log("Search response:", response.data);

        setResults(response.data.hits);
        setTotalHits(response.data.total);
        setAggregations(response.data.aggregations || {});
      } catch (err) {
        console.error("Search error:", err);
        setError(
          err.response?.data?.message || "Failed to fetch search results"
        );
        setResults([]);
        setTotalHits(0);
        setAggregations({});
      } finally {
        setLoading(false);
      }
    },
    [queryText, selectedYear, selectedType, selectedUniversity, pageSize]
  ); // Dependencies for useCallback

  // Initial search or when query/filters change
  useEffect(() => {
    performSearch(1); // Fetch first page on initial load or filter change
  }, [performSearch]); // Re-run if performSearch function identity changes (due to its deps)

  const handleSearchInputChange = (event) => {
    setQueryText(event.target.value);
  };

  const handleSearchSubmit = (event) => {
    event.preventDefault();
    performSearch(1); // Reset to first page on new search
  };

  const handlePageChange = (newPage) => {
    performSearch(newPage);
  };

  // --- Helper to render aggregations (Facets) ---
  const renderAggregation = (aggName, title, selectedValue, setter) => {
    const buckets = aggregations[aggName]?.buckets || [];
    if (buckets.length === 0) return null;

    return (
      <div className="facet">
        <h4>{title}</h4>
        <select value={selectedValue} onChange={(e) => setter(e.target.value)}>
          <option value="">All</option>
          {buckets.map((bucket) => (
            <option key={bucket.key} value={bucket.key}>
              {bucket.key} ({bucket.doc_count})
            </option>
          ))}
        </select>
      </div>
    );
  };

  const totalPages = Math.ceil(totalHits / pageSize);

  return (
    <div className="App">
      <header className="App-header">
        <h1>EUNICoast Papers Search Engine</h1>
        <form onSubmit={handleSearchSubmit} className="search-form">
          <input
            type="text"
            value={queryText}
            onChange={handleSearchInputChange}
            placeholder="Search papers..."
            className="search-input"
          />
          <button type="submit" className="search-button" disabled={loading}>
            {loading ? "Searching..." : "Search"}
          </button>
        </form>
      </header>

      <div className="container">
        <aside className="sidebar">
          <h3>Filters</h3>
          {renderAggregation(
            "papers_by_year",
            "Year",
            selectedYear,
            setSelectedYear
          )}
          {renderAggregation(
            "papers_by_type",
            "Type",
            selectedType,
            setSelectedType
          )}
          {renderAggregation(
            "papers_by_university",
            "University",
            selectedUniversity,
            setSelectedUniversity
          )}
          {/* Add more facets here */}
        </aside>

        <main className="results-main">
          {error && <p className="error-message">Error: {error}</p>}
          {loading && !results.length && <p>Loading results...</p>}
          {!loading && totalHits > 0 && <p>Found {totalHits} results.</p>}
          {!loading && totalHits === 0 && !error && <p>No results found.</p>}

          <div className="results-list">
            {results.map((paper) => (
              <div key={paper.id} className="paper-item">
                <h3>
                  <a href={paper.doi || "#"} target="_blank">
                    {paper.title || "No Title"}
                  </a>
                </h3>
                <p>
                  <strong>Authors:</strong> {paper.authors?.join(", ") || "N/A"}
                </p>
                <p>
                  <strong>Publication Date:</strong>{" "}
                  {paper.publication_date || "N/A"} | <strong>Type:</strong>{" "}
                  {paper.type || "N/A"}
                </p>
                <p>
                  <strong>Institutions:</strong>{" "}
                  {paper.institutions?.join(", ") || "N/A"}
                </p>
                <p>
                  <strong>EUNICoast University:</strong>{" "}
                  {paper.university_key || "N/A"}
                </p>
                <p>
                  <strong>Open Access:</strong>{" "}
                  {paper.open_access.is_oa ? "Yes" : "No"} (
                  {paper.open_access.oa_status})
                </p>
                <p className="abstract">
                  {paper.abstract
                    ? `${paper.abstract.substring(0, 500)}...`
                    : "No abstract available."}
                </p>
                {paper.score && (
                  <p>
                    <small>Score: {paper.score.toFixed(4)}</small>
                  </p>
                )}
              </div>
            ))}
          </div>

          {totalPages > 1 && (
            <div className="pagination">
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1 || loading}
              >
                Previous
              </button>
              <span>
                {" "}
                Page {currentPage} of {totalPages}{" "}
              </span>
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages || loading}
              >
                Next
              </button>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

export default App;
