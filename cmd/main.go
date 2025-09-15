package main

import (
	"fmt"
	"os"
	"paper-rank/internal/data"
	"paper-rank/internal/graph"
	"paper-rank/internal/search"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	maxPapers int
	outputDir string
	verbose   bool

	dampingFactor = 0.85
	maxIterations = 100
	tolerance     = 1e-6

	pagerankWeight  = 0.3
	relevanceWeight = 0.7
	maxResults      = 5
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "acl-ranker",
		Short: "ACL Paper Recommendation System using PageRank",
		Long: `A CLI tool that parses ACL papers, builds citation graphs, 
calculates PageRank scores, and provides intelligent paper search and ranking.`,
	}

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	rootCmd.AddCommand(parseCmd())
	rootCmd.AddCommand(buildCmd())
	rootCmd.AddCommand(rankCmd())
	rootCmd.AddCommand(searchCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parse [papers_file] [citations_file]",
		Short: "Parse ACL parquet files and extract paper data with citations",
		Long: `Parse both the ACL papers parquet file and citations parquet file from the data folder:
- Papers file: Contains paper metadata (title, authors, year, abstract, etc.)
- Citations file: Contains citation relationships between papers
- Clean and normalize the data
- Save as processed JSON for graph building`,
		Args: cobra.ExactArgs(2),
		Example: `  acl-ranker parse acl_papers.parquet acl_full_citations.parquet
  acl-ranker parse acl_papers.parquet acl_full_citations.parquet --max-papers 5000
  acl-ranker parse acl_papers.parquet acl_full_citations.parquet --output processed --verbose`,
		RunE: runParse,
	}

	cmd.Flags().IntVarP(&maxPapers, "max-papers", "m", 0, "Maximum number of papers to process (0 = all)")
	cmd.Flags().StringVarP(&outputDir, "output", "o", "processed", "Output directory for processed files")

	return cmd
}

func buildCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build citation graph from parsed data",
		Long:  "Build citation graph from parsed paper data and save to JSON format",
		RunE:  runBuild,
	}

	return cmd
}

func rankCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rank",
		Short: "Calculate PageRank scores for papers",
		Long:  "Calculate PageRank scores for all papers using the citation graph",
		RunE:  runRank,
	}

	return cmd
}

func searchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "search [query]",
		Short: "Search papers using PageRank-enhanced ranking",
		Long:  "Search for papers by keywords and rank results using PageRank scores",
		Args:  cobra.ExactArgs(1),
		RunE:  runSearch,
	}
	cmd.Flags().IntVarP(&maxResults, "max-results", "m", 5, "Maximum numbers of papers to show")

	return cmd
}

func runParse(cmd *cobra.Command, args []string) error {

	papersPath := filepath.Join("data", args[0])
	citationsPath := filepath.Join("data", args[1])

	// Check if input files exist
	if _, err := os.Stat(papersPath); os.IsNotExist(err) {
		return fmt.Errorf("papers file not found: %s", papersPath)
	}

	if _, err := os.Stat(citationsPath); os.IsNotExist(err) {
		return fmt.Errorf("citations file not found: %s", citationsPath)
	}

	// Create output directory
	outputPath := filepath.Join("data", outputDir)
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}
	outputFile := filepath.Join(outputPath, "papers.json")

	if verbose {
		fmt.Printf("Papers file: %s\n", papersPath)
		fmt.Printf("Citations file: %s\n", citationsPath)
		fmt.Printf("Output file: %s\n", outputFile)
		if maxPapers > 0 {
			fmt.Printf("Max papers: %d\n", maxPapers)
		} else {
			fmt.Printf("Max papers: unlimited\n")
		}
		fmt.Println("Starting parse operation...")
	}

	// run parse data
	parsedData, err := data.ParseACLData(papersPath, citationsPath, maxPapers)
	if err != nil {
		return fmt.Errorf("failed to parse ACL data: %v", err)
	}

	if err := data.SaveParsedData(parsedData, outputFile); err != nil {
		return fmt.Errorf("failed to save parsed data: %v", err)
	}

	fmt.Println("\nParse completed successfully!")
	data.PrintParsingStats(parsedData.Stats)
	fmt.Printf("\nOutput saved to: %s\n", outputFile)

	if stat, err := os.Stat(outputFile); err == nil {
		fmt.Printf("Output file size: %.2f MB\n", float64(stat.Size())/(1024*1024))
	}

	return nil
}

func runBuild(cmd *cobra.Command, args []string) error {
	// Default paths
	inputPath := filepath.Join("data", "processed", "papers.json")
	outputPath := filepath.Join("data", "processed", "graph.json")

	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return fmt.Errorf("input file not found: %s\nRun 'acl-ranker parse' first to create parsed data", inputPath)
	}

	if verbose {
		fmt.Printf("Input file: %s\n", inputPath)
		fmt.Printf("Output file: %s\n", outputPath)
		fmt.Println("Starting graph build operation...")
	}

	// Build the graph
	citationGraph, err := graph.BuildGraph(inputPath)
	if err != nil {
		return fmt.Errorf("failed to build graph: %v", err)
	}

	if err := graph.SaveGraph(citationGraph, outputPath); err != nil {
		return fmt.Errorf("failed to save graph: %v", err)
	}

	fmt.Println("\nGraph build completed successfully!")
	graph.PrintGraphStats(citationGraph.Stats)
	fmt.Printf("\nGraph saved to: %s\n", outputPath)

	if stat, err := os.Stat(outputPath); err == nil {
		fmt.Printf("Graph file size: %.2f MB\n", float64(stat.Size())/(1024*1024))
	}

	fmt.Println("\nTop 5 Most Cited Papers:")
	topPapers := citationGraph.GetMostCitedPapers(5)
	for i, paper := range topPapers {
		fmt.Printf("%d. %s (%d) - %d citations\n",
			i+1, paper.Title, paper.Year, paper.Citations)
	}

	return nil
}

func runRank(cmd *cobra.Command, args []string) error {
	inputPath := filepath.Join("data", "processed", "graph.json")
	outputPath := filepath.Join("data", "processed", "pagerank.json")

	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return fmt.Errorf("input file not found: %s\nRun 'acl-ranker build' first to create graph", inputPath)
	}

	if dampingFactor <= 0 || dampingFactor >= 1 {
		return fmt.Errorf("damping factor must be between 0 and 1, got: %.3f", dampingFactor)
	}
	if maxIterations <= 0 {
		return fmt.Errorf("max iterations must be positive, got: %d", maxIterations)
	}
	if tolerance <= 0 {
		return fmt.Errorf("tolerance must be positive, got: %.2e", tolerance)
	}

	if verbose {
		fmt.Printf("Input file: %s\n", inputPath)
		fmt.Printf("Output file: %s\n", outputPath)
		fmt.Printf("Damping factor: %.3f\n", dampingFactor)
		fmt.Printf("Max iterations: %d\n", maxIterations)
		fmt.Printf("Tolerance: %.2e\n", tolerance)
		fmt.Println("Starting PageRank calculation...")
	}

	citationGraph, err := graph.LoadGraph(inputPath)
	if err != nil {
		return fmt.Errorf("failed to load graph: %v", err)
	}

	config := graph.PageRankConfig{
		DampingFactor:  dampingFactor,
		MaxIterations:  maxIterations,
		Tolerance:      tolerance,
		HandleDangling: true,
	}

	result, err := graph.CalculatePageRank(citationGraph, config)
	if err != nil {
		return fmt.Errorf("failed to calculate PageRank: %v", err)
	}

	if err := graph.SavePageRankResult(result, outputPath); err != nil {
		return fmt.Errorf("failed to save PageRank results: %v", err)
	}

	fmt.Println("\nPageRank calculation completed successfully!")
	graph.PrintPageRankStats(result.Stats, result.Config)
	fmt.Printf("\nPageRank results saved to: %s\n", outputPath)

	if stat, err := os.Stat(outputPath); err == nil {
		fmt.Printf("PageRank file size: %.2f MB\n", float64(stat.Size())/(1024*1024))
	}

	graph.PrintTopPapers(result.Rankings, 10)

	graph.CompareWithCitations(result.Rankings, 5)

	return nil
}

func runSearch(cmd *cobra.Command, args []string) error {
	query := args[0]

	papersPath := filepath.Join("data", "processed", "papers_with_embeddings.json")
	pagerankPath := filepath.Join("data", "processed", "pagerank.json")
	cachePath := filepath.Join("data", "processed", "search_engine.cache.json")

	if _, err := os.Stat(papersPath); os.IsNotExist(err) {
		return fmt.Errorf("papers file with embeddings not found: %s\nPlease run the Python 'create_embeddings.py' script first", papersPath)
	}
	if _, err := os.Stat(pagerankPath); os.IsNotExist(err) {
		return fmt.Errorf("PageRank file not found: %s\nRun 'acl-ranker rank' first", pagerankPath)
	}

	if pagerankWeight < 0 || pagerankWeight > 1 {
		return fmt.Errorf("pagerank-weight must be between 0 and 1, got: %.3f", pagerankWeight)
	}
	if relevanceWeight < 0 || relevanceWeight > 1 {
		return fmt.Errorf("relevance-weight must be between 0 and 1, got: %.3f", relevanceWeight)
	}
	if maxResults <= 0 {
		return fmt.Errorf("max-results must be positive, got: %d", maxResults)
	}

	totalWeight := pagerankWeight + relevanceWeight
	if totalWeight <= 0 {

		fmt.Println("Warning: Weights sum to zero. Using defaults (Relevance: 0.8, PageRank: 0.2)")
		relevanceWeight = 0.8
		pagerankWeight = 0.2
	} else {

		pagerankWeight = pagerankWeight / totalWeight
		relevanceWeight = relevanceWeight / totalWeight
	}

	if verbose {
		fmt.Printf("Papers file: %s\n", papersPath)
		fmt.Printf("PageRank file: %s\n", pagerankPath)
		fmt.Printf("Query: \"%s\"\n", query)
		fmt.Printf("PageRank weight: %.3f\n", pagerankWeight)
		fmt.Printf("Relevance weight: %.3f\n", relevanceWeight)
		fmt.Printf("Max results: %d\n", maxResults)
		fmt.Println("Initializing search engine...")
	}

	config := search.SearchConfig{
		PageRankWeight:  pagerankWeight,
		RelevanceWeight: relevanceWeight,
		MaxResults:      maxResults,
		SnippetLength:   250,
	}

	engine, err := search.GetOrCreateEngine(papersPath, pagerankPath, cachePath, config)
	if err != nil {
		return fmt.Errorf("failed to create search engine: %v", err)
	}

	results, err := engine.Search(query)
	if err != nil {
		return fmt.Errorf("search failed: %v", err)
	}

	if len(results) == 0 {
		fmt.Printf("\nNo results found for: \"%s\"\n", query)
		fmt.Println("Try using different or broader terms.")
		return nil
	}

	search.PrintSearchResults(results, query)
	fmt.Printf("\nSearch completed with %.2f%% relevance + %.2f%% PageRank weighting\n",
		relevanceWeight*100, pagerankWeight*100)

	return nil
}
