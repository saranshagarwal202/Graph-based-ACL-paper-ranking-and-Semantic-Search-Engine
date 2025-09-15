package search

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"paper-rank/internal/data"
	"paper-rank/internal/graph"

	"github.com/mitchellh/go-wordwrap"
)

type SearchEngine struct {
	Papers   []data.Paper       `json:"papers"`
	PageRank map[string]float64 `json:"pagerank"`
	Config   SearchConfig       `json:"config"`
}

type SearchConfig struct {
	PageRankWeight  float64 `json:"pagerank_weight"`
	RelevanceWeight float64 `json:"relevance_weight"`
	MaxResults      int     `json:"max_results"`
	SnippetLength   int     `json:"snippet_length"`
}

type SearchResult struct {
	Paper          data.Paper `json:"paper"`
	Score          float64    `json:"score"`           // relevence score + pageRank score
	RelevanceScore float64    `json:"relevance_score"` // sentence similarity score
	PageRankScore  float64    `json:"pagerank_score"`  // PageRank score
	Snippet        string     `json:"snippet"`
}

type SearchQuery struct {
	Original   string `json:"original"`
	YearFilter int    `json:"year_filter"`
}

func DefaultSearchConfig() SearchConfig {
	return SearchConfig{
		PageRankWeight:  0.3,
		RelevanceWeight: 0.7,
		MaxResults:      20,
		SnippetLength:   200,
	}
}

func GetOrCreateEngine(papersPath, pagerankPath, cachePath string, config SearchConfig) (*SearchEngine, error) {
	if _, err := os.Stat(cachePath); err == nil {
		fmt.Printf("Loading pre-built search engine from: %s\n", cachePath)
		engine, err := LoadSearchEngine(cachePath)
		if err == nil {
			return engine, nil
		}
		fmt.Printf("Warning: failed to load cached engine: %v. Rebuilding...\n", err)
	}

	fmt.Println("No valid cache found. Building new search engine...")
	engine, err := NewSearchEngine(papersPath, pagerankPath, config)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Saving new engine to cache file: %s\n", cachePath)
	if err := SaveSearchEngine(engine, cachePath); err != nil {
		fmt.Printf("Warning: could not save search engine cache: %v\n", err)
	}

	return engine, nil
}

func NewSearchEngine(papersPath, pagerankPath string, config SearchConfig) (*SearchEngine, error) {
	fmt.Printf("Loading search data...\n")

	parsedData, err := data.LoadParsedData(papersPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load papers: %v", err)
	}

	pagerankResult, err := graph.LoadPageRankResult(pagerankPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load PageRank results: %v", err)
	}

	fmt.Printf("Loaded %d papers and PageRank scores\n", len(parsedData.Papers))

	engine := &SearchEngine{
		Papers:   parsedData.Papers,
		PageRank: pagerankResult.Scores,
		Config:   config,
	}

	fmt.Println("Search engine ready.")
	return engine, nil
}

func (se *SearchEngine) Search(queryStr string) ([]SearchResult, error) {
	query := se.parseQuery(queryStr)
	fmt.Printf("Searching for: \"%s\"\n", query.Original)

	// 1) get the embedding for the query
	queryEmbedding, err := getQueryEmbedding(query.Original)
	if err != nil {
		return nil, fmt.Errorf("could not get query embedding: %w", err)
	}

	// 2) score and rank all papers against the query embedding
	results := se.scoreAndRank(query, queryEmbedding)

	// 3) limit the results
	if len(results) > se.Config.MaxResults {
		results = results[:se.Config.MaxResults]
	}

	fmt.Printf("Returning top %d results\n", len(results))
	return results, nil
}

func (se *SearchEngine) parseQuery(queryStr string) SearchQuery {
	query := SearchQuery{
		Original: queryStr,
	}

	yearPattern := regexp.MustCompile(`\b(19|20)\d{2}\b`)
	if matches := yearPattern.FindAllString(queryStr, -1); len(matches) > 0 {
		lastYearStr := matches[len(matches)-1]
		var year int
		fmt.Sscanf(lastYearStr, "%d", &year)
		query.YearFilter = year
		query.Original = strings.TrimSpace(strings.ReplaceAll(query.Original, lastYearStr, ""))
	}

	return query
}

func (se *SearchEngine) scoreAndRank(query SearchQuery, queryEmbedding []float32) []SearchResult {
	results := make([]SearchResult, 0, len(se.Papers))

	for _, paper := range se.Papers {

		if query.YearFilter > 0 && paper.Year != query.YearFilter {
			continue
		}

		if len(paper.AbstractEmbedding) == 0 {
			continue
		}

		relevanceScore, err := cosineSimilarity(queryEmbedding, paper.AbstractEmbedding)
		if err != nil {
			continue
		}

		// scale cosine similarity from [-1, 1] to [0, 1] score.
		relevanceScore = (relevanceScore + 1) / 2
		pagerankScore := se.PageRank[paper.ID]
		combinedScore := se.Config.RelevanceWeight*relevanceScore + se.Config.PageRankWeight*pagerankScore

		snippet := se.createSnippet(paper)

		result := SearchResult{
			Paper:          paper,
			Score:          combinedScore,
			RelevanceScore: relevanceScore,
			PageRankScore:  pagerankScore,
			Snippet:        snippet,
		}
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results
}

func (se *SearchEngine) createSnippet(paper data.Paper) string {
	text := paper.Abstract
	if text == "" {
		text = paper.Title
	}

	if len(text) > se.Config.SnippetLength {
		if lastSpace := strings.LastIndex(text[:se.Config.SnippetLength], " "); lastSpace != -1 {
			return text[:lastSpace] + "..."
		}
		return text[:se.Config.SnippetLength] + "..."
	}
	return text
}

func getQueryEmbedding(query string) ([]float32, error) {
	//run python script in a new process
	cmd := exec.Command("python", "internal/sentenceEmbeddings/embed_query.py", query)

	output, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("embedding script failed: %s, stderr: %s", err, string(exitError.Stderr))
		}
		return nil, fmt.Errorf("failed to run embedding script: %w", err)
	}

	var embedding []float32
	if err := json.Unmarshal(output, &embedding); err != nil {
		return nil, fmt.Errorf("failed to parse embedding from python script: %w", err)
	}

	return embedding, nil
}

func cosineSimilarity(a, b []float32) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vectors have different lengths")
	}

	var dotProduct float64
	for i := 0; i < len(a); i++ {
		dotProduct += float64(a[i] * b[i])
	}

	return dotProduct, nil
}

func PrintSearchResults(results []SearchResult, query string) {
	fmt.Printf("\nSearch Results for: \"%s\"\n", query)
	fmt.Printf("Found %d results\n", len(results))
	fmt.Println("=" + strings.Repeat("=", 80))

	for i, result := range results {
		fmt.Printf("\n%d. %s (%d)\n", i+1, result.Paper.Title, result.Paper.Year)

		if len(result.Paper.Authors) > 0 {
			authors := result.Paper.Authors
			if len(authors) > 3 {
				authors = append(authors[:3], "et al.")
			}
			fmt.Printf("   Authors: %s\n", strings.Join(authors, ", "))
		}

		fmt.Printf("   Score: %.4f (Relevance: %.3f, PageRank: %.6f)\n",
			result.Score, result.RelevanceScore, result.PageRankScore)

		if result.Snippet != "" {
			wrappedSnippet := wordwrap.WrapString(result.Snippet, 80)
			indentedSnippet := strings.ReplaceAll(wrappedSnippet, "\n", "\n   ")
			fmt.Printf("   Snippet: %s\n", indentedSnippet)
		}
		fmt.Printf("   ID: %s\n", result.Paper.ID)
	}
	fmt.Println("\n" + strings.Repeat("=", 81))
}

func SaveSearchEngine(engine *SearchEngine, outputPath string) error {
	jsonData, err := json.MarshalIndent(engine, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal search engine: %v", err)
	}

	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write search engine file: %v", err)
	}

	return nil
}

func LoadSearchEngine(inputPath string) (*SearchEngine, error) {
	jsonData, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read search engine file: %v", err)
	}

	var engine SearchEngine
	if err := json.Unmarshal(jsonData, &engine); err != nil {
		return nil, fmt.Errorf("failed to unmarshal search engine: %v", err)
	}

	return &engine, nil
}
