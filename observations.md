# Obserwacje do magisterki

- Dokumenty w wielu różnych językach
-

- Zapis do pliku w formacie Parquet ze względu na:

## Parquet

**Advantages of Saving to a File First (e.g., Parquet):**

1.  **Persistence & Resilience:**

    - **Failure Recovery:** If the OpenSearch indexing process fails midway (network issues, cluster overload, mapping errors), you haven't lost the data fetched from OpenAlex. You can fix the issue and restart the indexing _from the saved file_ without needing to re-query the OpenAlex API (which saves time, API calls, and avoids potential rate limits).
    - **Backup:** You have a durable copy of the fetched data.

2.  **Decoupling:**

    - **Separation of Concerns:** It separates the data _fetching_ stage from the data _indexing_ stage. You can run the fetching script independently.
    - **Flexibility:** You can re-index the same data into OpenSearch later with different mappings or settings without re-fetching. You could also load the data into other systems (databases, data warehouses, analysis tools) from the file.

3.  **Efficiency (for the file format):**

    - **Parquet:** Is a columnar format, offering excellent compression and efficient read/write performance, especially for analytical queries or when loading data back into tools like Pandas. It handles nested structures reasonably well.

4.  **Data Validation/Transformation:**
    - You could potentially add a step after saving and before indexing to validate or further transform the data using tools optimized for file processing (like Spark, Dask, or Pandas on the saved file).

**Disadvantages of Saving to a File First:**

1.  **Added Complexity:** It introduces an extra step in your workflow (write to file, then read from file for indexing) and requires managing the intermediate file(s).
2.  **Disk I/O Overhead:** Writing to and reading from disk takes time and resources compared to processing directly from memory (though for large datasets, memory can be the bottleneck).
3.  **Disk Space:** Requires sufficient disk space to store the intermediate file(s).
4.  **Slightly Longer End-to-End Time (potentially):** The combined time of fetching + writing + reading + indexing might be slightly longer than fetching + indexing directly, assuming no failures and sufficient memory.

**Parquet vs. Iceberg:**

- **Parquet:** A file _format_. It's excellent for storing structured data efficiently. Libraries like `pandas` (with `pyarrow` or `fastparquet`) make it easy to work with. **This is likely the most practical choice for your use case.**
- **Iceberg:** A table _format specification_ often built _on top_ of Parquet files. It adds features like schema evolution, hidden partitioning, time travel (querying data as of a specific time), and ACID transactional guarantees, primarily designed for data lakehouse architectures. It's significantly more complex to set up and manage than just writing Parquet files. **Iceberg is probably overkill if your only goal is to transfer data to OpenSearch.**
