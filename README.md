# Graph based ACL paper ranking and Semantic Search Engine

This project is a command-line tool built in Go that provides a full pipeline for processing the ACL (Association for Computational Linguistics) Anthology dataset. It parses raw paper data, builds a citation graph, calculates paper importance using the PageRank algorithm, and offers a powerful semantic search engine to find and rank research papers.

The search functionality combines traditional graph-based authority scores (PageRank) with modern semantic relevance, calculated using sentence embeddings.

## Features

-   **Efficient Data Parsing**: Handles large Parquet datasets containing paper metadata and citations using Go's Apache Arrow library.
-   **Citation Graph Construction**: Builds a directed graph representing the citation network between all processed papers.
-   **PageRank Implementation**: Calculates PageRank scores from scratch to determine the authority and influence of each paper in the network.
-   **Hybrid Semantic Search**: Implements a sophisticated search mechanism that ranks results based on a weighted combination of:
    1.  **Semantic Relevance**: The contextual similarity between a search query and a paper's abstract, calculated using sentence embeddings.
    2.  **PageRank Score**: The paper's authority within the citation graph.
-   **Go & Python Interoperability**: The Go application calls a Python script to generate sentence embeddings for search queries on the fly.
-   **Persistent Caching**: The search engine index is automatically cached after the first run for significantly faster subsequent startups.
-   **Modular CLI**: A clean command-line interface built with Cobra, separating the distinct stages of the data pipeline (parse, build, rank, search).

## Architecture & Data Flow

The tool operates as a multi-stage pipeline. Each command consumes the output of the previous one, ensuring a clear and reproducible workflow.

1.  **`parse`**:
    -   **Input**: Raw Parquet files (`papers.parquet`, `citations.parquet`).
    -   **Process**: Cleans data, extracts paper metadata, and builds citation lists.
    -   **Output**: A clean `papers.json` file.

2.  **`create_embeddings.py` (Python Script)**:
    -   **Input**: The `papers.json` file.
    -   **Process**: Generates sentence embeddings for the abstract of each paper using the `sentence-transformers` library.
    -   **Output**: An augmented `papers_with_embeddings.json` file.

3.  **`build`**:
    -   **Input**: The `papers.json` file.
    -   **Process**: Constructs the citation graph with nodes, edges, and pre-calculated degrees.
    -   **Output**: A `graph.json` file.

4.  **`rank`**:
    -   **Input**: The `graph.json` file.
    -   **Process**: Runs the iterative PageRank algorithm on the graph.
    -   **Output**: A `pagerank.json` file containing the final scores for each paper.

5.  **`search`**:
    -   **Input**: The `papers_with_embeddings.json` and `pagerank.json` files.
    -   **Process**: Loads all data, generates an embedding for the search query, calculates relevance and PageRank scores for all papers, and returns a sorted list. Caches the engine for future runs.
    -   **Output**: A ranked list of relevant papers printed to the console.

## Prerequisites

-   Go (version 1.20 or later)
-   Python (version 3.9 or later)
-   The required Python libraries:
    ```bash
    pip install sentence-transformers torch huggingface-hub
    ```
-   A Hugging Face account and an access token to download the embedding model.

## Quick Start

1.  **Place Data**:
    Download the ACL Parquet files from [WINGNUS/ACL-OCL](https://huggingface.co/datasets/WINGNUS/ACL-OCL) and place them inside a `data/` directory in the project root.

2.  **Run the Full Pipeline**:
    Execute the commands in the following order.

    **Step 1: Build the go module**
    ```bash
    go build -o acl_ranker ./cmd
    ```

    **Step 1: Parse the raw data**
    ```bash
    ./acl_ranker parse acl-publication-info.74k.v2.parquet acl_full_citations.parquet
    ```
    This will create `data/processed/papers.json`.

    **Step 2: Generate embeddings**
    ```bash
    python create_embeddings.py
    ```
    This will read `papers.json` and create `data/processed/papers_with_embeddings.json`.

    **Step 3: Build the citation graph**
    ```bash
    ./acl_ranker build
    ```
    This will create `data/processed/graph.json`.

    **Step 4: Calculate PageRank scores**
    ```bash
    ./acl_ranker rank
    ```
    This will create `data/processed/pagerank.json`.

    **Step 5: Perform a search**
    ```bash
    ./acl_ranker search "hallucination large language model"
    ```
    The first time you run this, it will build and save `data/processed/search_engine.cache.json`. Subsequent searches will be much faster.


