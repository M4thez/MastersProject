import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import "./AppNew.css";
import "./variables.css";
import "./toggleSwitch.css";
import universitiesNameMap from "./utils/universitiesNameMap";

const BACKEND_BASE_URL = "http://82.145.73.10:3001/api/search/"; // Node.js backend URL

// Define default sort options for each type
const defaultSortOptions = {
  papers: { field: "_score", order: "desc" },
  authors: { field: "display_name.keyword", order: "asc" }, // Example
  projects: { field: "startDate", order: "desc" }, // Example
};

function App() {
  const [queryText, setQueryText] = useState("");
  const [results, setResults] = useState([]);
  const [totalHits, setTotalHits] = useState(0);
  const [aggregations, setAggregations] = useState({});
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [searchType, setSearchType] = useState("papers");

  // Sort selection
  const [selectedSort, setSelectedSort] = useState(
    defaultSortOptions[searchType].field
  );
  const [sortOrder, setSortOrder] = useState(
    defaultSortOptions[searchType].order
  );

  // --- Filters State ---
  const [paperFilters, setPaperFilters] = useState({
    type: "",
    university_key: "",
  });
  const [authorFilters, setAuthorFilters] = useState({
    university_key: "",
    lki_type: "",
  }); // Example
  const [projectFilters, setProjectFilters] = useState({
    status: "",
    funder_name: "",
  }); // Example

  // const [selectedType, setSelectedType] = useState("");
  // const [selectedUniversity, setSelectedUniversity] = useState("");

  const performSearch = useCallback(
    async (pageToFetch = 1, currentSearchType = searchType) => {
      setLoading(true);
      setError(null);
      setCurrentPage(pageToFetch);

      // const full_url = BACKEND_BASE_URL + "papers";
      let endpoint = `${BACKEND_BASE_URL}${currentSearchType}`;
      let currentActiveFilters = {};
      let sortByPayload = null;

      switch (currentSearchType) {
        case "papers":
          currentActiveFilters = {
            ...(paperFilters.type && { type: paperFilters.type }),
            ...(paperFilters.university_key && {
              university_key: paperFilters.university_key,
            }),
          };
          sortByPayload = selectedSort
            ? { field: selectedSort, order: sortOrder }
            : defaultSortOptions.papers;
          break;
        case "authors":
          currentActiveFilters = {
            ...(authorFilters.university_key && {
              university: authorFilters.university_key,
            }),
            ...(authorFilters.lki_type && { lki_type: authorFilters.lki_type }),
          };
          sortByPayload = selectedSort
            ? { field: selectedSort, order: sortOrder }
            : defaultSortOptions.authors;
          break;
        case "projects":
          currentActiveFilters = {
            ...(projectFilters.status && { status: projectFilters.status }),
            ...(projectFilters.funder_name && {
              funder_name: projectFilters.funder_name,
            }),
          };
          sortByPayload = selectedSort
            ? { field: selectedSort, order: sortOrder }
            : defaultSortOptions.projects;
          break;
        default:
          console.warn("DIDN'T CATCH A CASE IN A SWITCH");
          break;
      }

      try {
        const response = await axios.post(endpoint, {
          queryText: queryText,
          filters: currentActiveFilters,
          page: pageToFetch,
          pageSize: pageSize,
          sortBy: sortByPayload,
        });
        console.log(`${currentSearchType} Search response:`, response.data);

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
      pageSize,
      searchType,
      selectedSort,
      sortOrder,
      paperFilters,
      authorFilters,
      projectFilters,
    ]
  ); // Dependencies for useCallback

  // Initial search or when query/filters change
  useEffect(() => {
    performSearch(1, searchType); // Fetch first page on initial load or filter change
  }, [performSearch, searchType]); // Re-run if performSearch function identity changes (due to its deps)

  // Effect to reset sort and filters when searchType changes
  useEffect(() => {
    setQueryText(""); // Optionally reset query text
    setResults([]);
    setTotalHits(0);
    setAggregations({});
    setCurrentPage(1);

    // Reset filters for each type
    setPaperFilters({ type: "", university_key: "" });
    setAuthorFilters({ university_key: "", lki_type: "" }); // Reset specific author filters
    setProjectFilters({ status: "", funder_name: "" }); // Reset specific project filters

    // Set default sort for the new type
    setSelectedSort(defaultSortOptions[searchType].field);
    setSortOrder(defaultSortOptions[searchType].order);

    // performSearch will be called by the effect below due to searchType change
  }, [searchType]);

  const handleSearchSubmit = (event) => {
    event.preventDefault();
    setQueryText(event.target[0].value);
    performSearch(1, searchType); // Reset to first page on new search
  };

  const handlePageChange = (newPage) => {
    performSearch(newPage, searchType);
  };

  // --- Helper to render aggregations---
  const renderAggregation = (
    aggName,
    title,
    selectedValue,
    setterForKey,
    filterObjectSetter,
    valueMap = null
  ) => {
    const buckets = aggregations[aggName]?.buckets || [];
    if (buckets.length === 0) return null;

    return (
      <div className="filter-single">
        <h4>{title}</h4>
        <select
          value={selectedValue}
          onChange={(e) => {
            filterObjectSetter((prev) => ({
              ...prev,
              [setterForKey]: e.target.value,
            }));
          }}
        >
          <option value="">All</option>
          {buckets.map((bucket) => {
            const displayName =
              valueMap && valueMap[bucket.key]
                ? valueMap[bucket.key]
                : bucket.key;
            return (
              <option key={bucket.key} value={bucket.key}>
                {" "}
                {/* Value is the key for filtering */}
                {displayName} ({bucket.doc_count})
              </option>
            );
          })}
        </select>
      </div>
    );
  };

  // --- Sorting Options based on searchType ---
  const paperSortOptions = [
    { value: "_score", label: "Relevance" },
    { value: "publication_date", label: "Date" },
    { value: "cited_by_count", label: "Citations" },
    { value: "fwci", label: "FWCI" },
  ];
  const authorSortOptions = [
    { value: "_score", label: "Relevance" },
    { value: "display_name.keyword", label: "Name (A-Z)" }, // Assuming mapping
    { value: "works_count", label: "Works Count" },
    { value: "cited_by_count", label: "Author Citations" },
  ];
  const projectSortOptions = [
    { value: "_score", label: "Relevance" },
    { value: "startDate", label: "Start Date" },
    // Add more project-specific sort options
  ];

  let currentSortOptions = paperSortOptions;
  if (searchType === "authors") currentSortOptions = authorSortOptions;
  if (searchType === "projects") currentSortOptions = projectSortOptions;

  const renderSortingControls = () => {
    return (
      <div className="sort-controls">
        <label htmlFor="sortFieldSelect">Sort by:</label>
        <select
          id="sortFieldSelect"
          value={selectedSortField}
          onChange={(e) => setSelectedSort(e.target.value)}
        >
          {currentSortOptions.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
        <label className="sort-order-toggle" htmlFor="sortOrderToggle">
          <input
            title="Toggle sort order"
            type="checkbox"
            id="sortOrderToggle"
            name="sortOrderToggle"
            checked={sortOrder === "desc"} // "desc" is checked (down arrow)
            onChange={(e) => setSortOrder(e.target.checked ? "desc" : "asc")}
          />
          <span className="sort-order-slider"></span>
        </label>
        <span className="sort-order-text">
          {sortOrder === "desc" ? "\u25BC" : "\u25B2"} {/* Down / Up Arrow */}
        </span>
        <button
          title="Reset sorting"
          type="button"
          className="reset-button"
          onClick={() => {
            const defaults = defaultSortOptions[searchType];
            setSelectedSort(defaults.field);
            setSortOrder(defaults.order);
          }}
        >
          ↺
        </button>
      </div>
    );
  };

  // --- Function to render individual result items ---
  const renderResultItem = (item) => {
    if (searchType === "papers") {
      return (
        <div key={item.id} className="paper-item">
          <h3>
            <a
              href={item.doi || item.oa_url || "#"}
              target="_blank"
              rel="noopener noreferrer"
            >
              {item.title || "No Title"}
            </a>
          </h3>
          <p>
            <strong>Authors:</strong> {item.authors?.join(", ") || "N/A"}
          </p>
          <p>
            <strong>Date:</strong> {item.publication_date || "N/A"} |{" "}
            <strong>Type:</strong> {item.type || "N/A"} | <strong>Lang:</strong>{" "}
            {item.language || "N/A"}
          </p>
          <p>
            <strong>Citations:</strong> {item.cited_by_count || 0} |{" "}
            <strong>FWCI:</strong>{" "}
            {item.fwci !== null && item.fwci !== undefined
              ? item.fwci.toFixed(2)
              : "N/A"}
          </p>
          <p>
            <strong>Institutions:</strong>{" "}
            {item.institutions?.join(", ") || "N/A"}
          </p>
          <p>
            <strong>EUNICoast University:</strong>{" "}
            {universitiesNameMap[item.university_key] ||
              item.university_key ||
              "N/A"}
          </p>
          <p>
            <strong>Open Access:</strong>{" "}
            {item.open_access?.is_oa ? "Yes" : "No"} (
            {item.open_access?.oa_status || "N/A"})
          </p>
          <p>
            <strong>Abstract: </strong>
            {item.abstract ? `${item.abstract.substring(0, 300)}...` : "--"}
          </p>
          {item.score && (
            <p>
              <small>Score: {item.score.toFixed(2)}</small>
            </p>
          )}
        </div>
      );
    } else if (searchType === "authors") {
      return (
        <div key={item.id} className="author-item paper-item">
          {" "}
          {/* Reuse paper-item style or create author-item */}
          <h3>{item.display_name || "No Name"}</h3>
          <p>
            <strong>ORCID:</strong>{" "}
            {item.orcid ? (
              <a href={item.orcid} target="_blank" rel="noopener noreferrer">
                {item.orcid}
              </a>
            ) : (
              "N/A"
            )}
          </p>
          <p>
            <strong>Works:</strong> {item.works_count || 0} |{" "}
            <strong>Cited By:</strong> {item.cited_by_count || 0}
          </p>
          <p>
            <strong>H-Index:</strong> {item.summary_stats?.h_index || "N/A"} |{" "}
            <strong>i10-Index:</strong> {item.summary_stats?.i10_index || "N/A"}
          </p>
          <p>
            <strong>Last Known Uni:</strong>{" "}
            {item.last_known_institutions?.[0]?.display_name || "N/A"} (
            {item.university_key || "N/A"})
          </p>
          {item.x_concepts && item.x_concepts.length > 0 && (
            <p>
              <strong>Top Concepts:</strong>{" "}
              {item.x_concepts
                .slice(0, 3)
                .map((c) => c.display_name)
                .join(", ")}
            </p>
          )}
          {item.score && (
            <p>
              <small>Score: {item.score.toFixed(2)}</small>
            </p>
          )}
        </div>
      );
    } else if (searchType === "projects") {
      return (
        <div key={item.id} className="project-item paper-item">
          {" "}
          {/* Reuse paper-item style or create project-item */}
          <h3>{item.title || "No Title"}</h3>
          <p>
            <strong>Acronym:</strong> {item.acronym || "N/A"} |{" "}
            <strong>Code:</strong> {item.code || "N/A"}
          </p>
          <p>
            <strong>Dates:</strong> {item.startDate || "N/A"} to{" "}
            {item.endDate || "N/A"}
          </p>
          <p>
            <strong>Call ID:</strong> {item.callIdentifier || "N/A"}
          </p>
          {item.fundings && item.fundings.length > 0 && (
            <p>
              <strong>Funder(s):</strong>{" "}
              {item.fundings.map((f) => f.name?.keyword || f.name).join(", ")}
            </p> // Adjust if name is object
          )}
          <p>
            <strong>Summary: </strong>
            {item.summary ? `${item.summary.substring(0, 300)}...` : "--"}
          </p>
          {item.score && (
            <p>
              <small>Score: {item.score.toFixed(2)}</small>
            </p>
          )}
        </div>
      );
    }
    return null;
  };

  const totalPages = Math.ceil(totalHits / pageSize);

  return (
    <div className="App">
      <header className="App-header">
        <h1>EUNICoast Search Engine</h1>
        {/* --- Search Type Selector --- */}
        <div className="search-type-selector">
          <button
            onClick={() => setSearchType("papers")}
            className={searchType === "papers" ? "active" : ""}
          >
            Papers
          </button>
          <button
            onClick={() => setSearchType("authors")}
            className={searchType === "authors" ? "active" : ""}
          >
            Authors
          </button>
          <button
            onClick={() => setSearchType("projects")}
            className={searchType === "projects" ? "active" : ""}
          >
            Projects
          </button>
        </div>

        <form onSubmit={handleSearchSubmit} className="search-form">
          <input
            type="text"
            key={searchType} // Add key to force re-render and clear input on searchType change
            // value={queryText} // Make it controlled if you prefer
            placeholder={`Search ${searchType}...`}
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

        {renderSortingControls()}

        {/* Filters Container - Rendered conditionally based on searchType */}
        <div className="filters-container">
          <div className="filters-header">
            <h3>Filters</h3>
            <button
              title="Reset filters"
              type="button"
              className="reset-button"
              onClick={() => {
                if (searchType === "papers")
                  setPaperFilters({ type: "", university_key: "" });
                else if (searchType === "authors")
                  setAuthorFilters({ university_key: "", lki_type: "" });
                else if (searchType === "projects")
                  setProjectFilters({ status: "", funder_name: "" });
              }}
            >
              ↺
            </button>
          </div>
          {searchType === "papers" && (
            <>
              {renderAggregation(
                "papers_by_type",
                "Type",
                paperFilters.type,
                "type",
                setPaperFilters
              )}
              {renderAggregation(
                "papers_by_university",
                "University",
                paperFilters.university_key,
                "university_key",
                setPaperFilters,
                universitiesNameMap
              )}
              {/* Add paper-specific year filter if needed, e.g., using a different agg name */}
              {renderAggregation(
                "papers_by_year",
                "Year",
                paperFilters.year,
                "year",
                setPaperFilters
              )}
            </>
          )}
          {searchType === "authors" && (
            <>
              {/* Pass appropriate filter state and setter keys */}
              {renderAggregation(
                "authors_by_university",
                "University",
                authorFilters.university_key,
                "university_key",
                setAuthorFilters,
                universitiesNameMap
              )}
              {renderAggregation(
                "authors_by_lki_type",
                "Institution Type",
                authorFilters.lki_type,
                "lki_type",
                setAuthorFilters
              )}
            </>
          )}
          {searchType === "projects" && (
            <>
              {renderAggregation(
                "projects_by_status",
                "Status",
                projectFilters.status,
                "status",
                setProjectFilters
              )}
              {renderAggregation(
                "projects_by_funder",
                "Funder",
                projectFilters.funder_name,
                "funder_name",
                setProjectFilters
              )}
              {/* You might need a different renderAggregation for date_histogram like project years */}
              {renderAggregation(
                "projects_by_year_corrected",
                "Start Year",
                projectFilters.startYear,
                "startYear",
                setProjectFilters
              )}
            </>
          )}
        </div>

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
        {loading && results.length === 0 && <p>Loading...</p>}
        {!loading && totalHits === 0 && !error && (
          <span>
            No results found for "{queryText}"
            {searchType === "papers" &&
              (paperFilters.type || paperFilters.university_key) &&
              " with applied paper filters."}
            {searchType === "authors" &&
              (authorFilters.university_key || authorFilters.lki_type) &&
              " with applied author filters."}
            {searchType === "projects" &&
              (projectFilters.status || projectFilters.funder_name) &&
              " with applied project filters."}
          </span>
        )}
        {results.length > 0 && (
          <div className="results-list">
            <h2 className="search-results-header">
              Search Results for{" "}
              {searchType.charAt(0).toUpperCase() + searchType.slice(1)}
            </h2>
            {results.map((item) => renderResultItem(item))}
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
