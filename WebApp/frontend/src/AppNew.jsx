import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import "./AppNew.css";
import "./variables.css";
import "./toggleSwitch.css";
import universitiesNameMap from "./utils/universitiesNameMap";

const BACKEND_BASE_URL = "http://82.145.73.10:3001/api/search/"; // Node.js backend URL

function App() {
  const [queryText, setQueryText] = useState("");
  const [results, setResults] = useState([]);
  const [totalHits, setTotalHits] = useState(0);
  const [aggregations, setAggregations] = useState({});
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Sort selection
  const [selectedSort, setSelectedSort] = useState("_score");
  const [sortOrder, setSortOrder] = useState("desc");

  // --- Filters State ---
  const [selectedType, setSelectedType] = useState("");
  const [selectedUniversity, setSelectedUniversity] = useState("");

  const performPapersSearch = useCallback(
    async (pageToFetch = 1) => {
      full_url = BACKEND_BASE_URL + "papers";
      setLoading(true);
      setError(null);
      setCurrentPage(pageToFetch);

      const filters = {};
      if (selectedType) filters.type = selectedType;
      if (selectedUniversity) filters.university_key = selectedUniversity;
      // Add more filters here

      try {
        const response = await axios.post(BACKEND_BASE_URL, {
          queryText: queryText,
          filters: filters,
          page: pageToFetch,
          pageSize: pageSize,
          sortBy: { field: selectedSort, order: sortOrder },
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
    [
      queryText,
      selectedType,
      selectedUniversity,
      selectedSort,
      sortOrder,
      pageSize,
    ]
  ); // Dependencies for useCallback

  // Initial search or when query/filters change
  useEffect(() => {
    performPapersSearch(1); // Fetch first page on initial load or filter change
  }, [performPapersSearch]); // Re-run if performSearch function identity changes (due to its deps)

  // const handleSearchInputChange = (event) => {
  // };

  const handleSearchSubmit = (event) => {
    setQueryText(event.target[0].value);
    event.preventDefault();
    performPapersSearch(1); // Reset to first page on new search
  };

  const handlePageChange = (newPage) => {
    performPapersSearch(newPage);
  };

  // --- Helper to render aggregations---
  const renderAggregation = (aggName, title, selectedValue, setter) => {
    const buckets = aggregations[aggName]?.buckets || [];
    if (buckets.length === 0) return null;

    return (
      <div className="filter-single">
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

  const renderSorting = (sortFieldSetter, sortOrderSetter) => {
    return (
      <div className="sort-controls">
        <label htmlFor="sortFieldSelect">Sort by:</label>
        <select
          id="sortFieldSelect"
          value={selectedSort}
          onChange={(e) => sortFieldSetter(e.target.value)}
        >
          <option value="_score">Relevance</option>
          <option value="publication_date">Date</option>
          <option value="cited_by_count">Citations</option>
          <option value="fwci">FWCI</option>
        </select>
        <label className="sort-order-toggle" htmlFor="sortOrderToggle">
          <input
            title="Toggle sort order"
            type="checkbox"
            id="sortOrderToggle"
            name="sortOrderToggle"
            checked={sortOrder === "desc"}
            onChange={(e) => sortOrderSetter(e.target.checked ? "desc" : "asc")}
          />
          <span className="sort-order-slider"></span>
        </label>
        <span className="sort-order-text">
          {sortOrder === "desc" ? "\u25BC" : "\u25B2"}
        </span>
        <button
          title="Reset sorting"
          type="button"
          className="reset-button"
          onClick={() => {
            setSortOrder("desc");
            setSelectedSort("_score");
          }}
        >
          &#8634;
        </button>
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
            // value={queryText}
            // onChange={handleSearchInputChange}
            placeholder="Search papers..."
            className="search-input"
          />
          <button
            title="Perform search"
            type="submit"
            className="search-button"
            disabled={loading}
          >
            {loading ? "Searching..." : "Search"}
          </button>
        </form>
        {/* Sorting */}
        {renderSorting(setSelectedSort, setSortOrder)}
        {/* Filters */}
        <div className="filters-container">
          <div className="filters-header">
            <h3>Filters</h3>
            <button
              title="Reset filters"
              type="button"
              className="reset-button"
              onClick={() => {
                setSelectedType("");
                setSelectedUniversity("");
              }}
            >
              &#8634;
            </button>
          </div>
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
        </div>
        {/* Search results info */}

        <div className="search-info">
          {error && <span className="error-message">Error: {error}</span>}
          {!loading && totalHits > 0 && results.length > 0 && (
            <span>
              Found <strong>{totalHits}</strong> results.
            </span>
          )}
          {!loading && totalHits === 0 && !error && (
            <span>
              No results found.{" "}
              {(selectedType || selectedUniversity) &&
                "Applied filters: " + selectedType + " " + selectedUniversity}
            </span>
          )}
        </div>
      </header>
      <main className="results-main">
        {results.length > 0 && (
          <div className="results-list">
            <h2 className="search-results-header">Search Results</h2>
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
                  <strong>Publication Date: </strong>
                  {paper.publication_date || "N/A"} | <strong>Type: </strong>
                  {paper.type || "N/A"} | <strong>Language: </strong>{" "}
                  {paper.language || "N/A"}
                </p>
                <p>
                  <strong>Citations: </strong> {paper.cited_by_count || 0} |{" "}
                  <strong>FWCI: </strong> {paper.fwci || 0}
                </p>
                <p>
                  <strong>Institutions:</strong>{" "}
                  {paper.institutions?.join(", ") || "N/A"}
                </p>
                <p>
                  <strong>EUNICoast University:</strong>{" "}
                  {universitiesNameMap[paper.university_key] || "N/A"} (
                  {paper.university_key})
                </p>
                <p>
                  <strong>Open Access:</strong>{" "}
                  {paper.open_access.is_oa ? "Yes" : "No"} (
                  {paper.open_access.oa_status})
                </p>
                <p>
                  <strong>Abstract: </strong>
                  {paper.abstract
                    ? `${paper.abstract.substring(0, 500)}...`
                    : "--"}
                </p>
                {paper.score && (
                  <p>
                    <small>Score: {paper.score.toFixed(2)}</small>
                  </p>
                )}
              </div>
            ))}
          </div>
        )}
        {totalPages > 1 && results.length > 0 && (
          <div className="pagination">
            <button
              onClick={() => handlePageChange(currentPage - 1)}
              disabled={currentPage === 1 || loading}
            >
              &#9664;
            </button>
            <span>
              {" "}
              Page {currentPage} of {totalPages}{" "}
            </span>
            <button
              onClick={() => handlePageChange(currentPage + 1)}
              disabled={currentPage === totalPages || loading}
            >
              &#9654;
            </button>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
