package graph

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type PageRankResult struct {
	Scores   map[string]float64 `json:"scores"` // paper_id -> PageRank score
	Config   PageRankConfig     `json:"config"`
	Stats    PageRankStats      `json:"stats"`
	Rankings []PaperScore       `json:"rankings"`
}

type PageRankConfig struct {
	DampingFactor  float64 `json:"damping_factor"`
	MaxIterations  int     `json:"max_iterations"`
	Tolerance      float64 `json:"tolerance"`
	HandleDangling bool    `json:"handle_dangling"`
}

type PageRankStats struct {
	Iterations      int     `json:"iterations"`
	Converged       bool    `json:"converged"`
	ComputationTime string  `json:"computation_time"`
	DanglingNodes   int     `json:"dangling_nodes"`
	MaxScoreChange  float64 `json:"max_score_change"`
	TopPaper        string  `json:"top_paper"`
	TopScore        float64 `json:"top_score"`
}

type PaperScore struct {
	PaperID   string  `json:"paper_id"`
	Title     string  `json:"title"`
	Year      int     `json:"year"`
	Score     float64 `json:"score"`
	Citations int     `json:"citations"`
}

func CalculatePageRank(graph *Graph, config PageRankConfig) (*PageRankResult, error) {
	startTime := time.Now()

	fmt.Printf("Starting PageRank calculation...\n")
	fmt.Printf("Damping factor: %.2f\n", config.DampingFactor)
	fmt.Printf("Max iterations: %d\n", config.MaxIterations)
	fmt.Printf("Tolerance: %.2e\n", config.Tolerance)

	numNodes := len(graph.Nodes)
	if numNodes == 0 {
		return nil, fmt.Errorf("graph has no nodes")
	}

	nodeIndex := make(map[string]int)
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)

	initialScore := 1.0 / float64(numNodes)
	for i, node := range graph.Nodes {
		nodeIndex[node.ID] = i
		scores[i] = initialScore
	}

	danglingNodes := []int{}
	for i, node := range graph.Nodes {
		if graph.OutDegree[node.ID] == 0 {
			danglingNodes = append(danglingNodes, i)
		}
	}

	fmt.Printf("Found %d dangling nodes (%.1f%%)\n",
		len(danglingNodes),
		float64(len(danglingNodes))/float64(numNodes)*100)

	var iteration int
	var converged bool
	var maxScoreChange float64

	for iteration = 0; iteration < config.MaxIterations; iteration++ {
		// for dangling nodes distribute their score evenly
		danglingContribution := 0.0
		if config.HandleDangling {
			for _, danglingIdx := range danglingNodes {
				danglingContribution += scores[danglingIdx]
			}
			danglingContribution /= float64(numNodes)
		}

		for i := range newScores {
			// 1) teleportation probability
			newScores[i] = (1.0 - config.DampingFactor) / float64(numNodes)

			// 2) dangling node contribution
			if config.HandleDangling {
				newScores[i] += config.DampingFactor * danglingContribution
			}
		}

		// contributions from incoming links
		for _, edge := range graph.Edges {
			fromIdx := nodeIndex[edge.From]
			toIdx := nodeIndex[edge.To]

			outDegree := graph.OutDegree[edge.From]
			if outDegree > 0 {
				contribution := config.DampingFactor * scores[fromIdx] / float64(outDegree)
				newScores[toIdx] += contribution
			}
		}

		// check for convergence
		maxScoreChange = 0.0
		for i := range scores {
			change := math.Abs(newScores[i] - scores[i])
			if change > maxScoreChange {
				maxScoreChange = change
			}
		}

		scores, newScores = newScores, scores

		if (iteration+1)%10 == 0 {
			fmt.Printf("Iteration %d: max score change = %.2e\n", iteration+1, maxScoreChange)
		}

		if maxScoreChange < config.Tolerance {
			converged = true
			break
		}
	}

	computationTime := time.Since(startTime)

	fmt.Printf("PageRank completed in %d iterations (%.2f seconds)\n",
		iteration+1, computationTime.Seconds())

	if converged {
		fmt.Printf("Converged with max score change: %.2e\n", maxScoreChange)
	} else {
		fmt.Printf("Did not converge after %d iterations\n", config.MaxIterations)
	}

	scoreMap := make(map[string]float64)
	var topScore float64
	var topPaper string

	for i, node := range graph.Nodes {
		scoreMap[node.ID] = scores[i]
		if scores[i] > topScore {
			topScore = scores[i]
			topPaper = node.ID
		}
	}

	rankings := createRankings(graph, scoreMap)

	stats := PageRankStats{
		Iterations:      iteration + 1,
		Converged:       converged,
		ComputationTime: computationTime.String(),
		DanglingNodes:   len(danglingNodes),
		MaxScoreChange:  maxScoreChange,
		TopPaper:        topPaper,
		TopScore:        topScore,
	}

	result := &PageRankResult{
		Scores:   scoreMap,
		Config:   config,
		Stats:    stats,
		Rankings: rankings,
	}

	return result, nil
}

func createRankings(graph *Graph, scores map[string]float64) []PaperScore {
	rankings := make([]PaperScore, 0, len(graph.Nodes))

	for _, node := range graph.Nodes {
		paperScore := PaperScore{
			PaperID:   node.ID,
			Title:     node.Title,
			Year:      node.Year,
			Score:     scores[node.ID],
			Citations: graph.InDegree[node.ID],
		}
		rankings = append(rankings, paperScore)
	}
	sort.Slice(rankings, func(i, j int) bool {
		return rankings[i].Score > rankings[j].Score
	})

	return rankings
}

func SavePageRankResult(result *PageRankResult, outputPath string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal PageRank result to JSON: %v", err)
	}

	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write PageRank file: %v", err)
	}

	return nil
}

func LoadPageRankResult(inputPath string) (*PageRankResult, error) {
	jsonData, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read PageRank file: %v", err)
	}

	var result PageRankResult
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PageRank data: %v", err)
	}

	return &result, nil
}

func PrintPageRankStats(stats PageRankStats, config PageRankConfig) {
	fmt.Println("\n=== PageRank Results ===")
	fmt.Printf("Algorithm converged: %v\n", stats.Converged)
	fmt.Printf("Iterations completed: %d/%d\n", stats.Iterations, config.MaxIterations)
	fmt.Printf("Computation time: %s\n", stats.ComputationTime)
	fmt.Printf("Final convergence: %.2e (target: %.2e)\n", stats.MaxScoreChange, config.Tolerance)
	fmt.Println()

	fmt.Printf("Dangling nodes: %d\n", stats.DanglingNodes)
	fmt.Printf("Highest PageRank: %.6f (paper: %s)\n", stats.TopScore, stats.TopPaper)
	fmt.Println()

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Damping factor: %.2f\n", config.DampingFactor)
	fmt.Printf("  Handle dangling nodes: %v\n", config.HandleDangling)
	fmt.Println("=======================")
}

func PrintTopPapers(rankings []PaperScore, n int) {
	if n > len(rankings) {
		n = len(rankings)
	}

	fmt.Printf("\nTop %d Papers by PageRank:\n", n)
	fmt.Println("Rank | Score    | Citations | Year | Title")
	fmt.Println("-----|----------|-----------|------|--------------------------------")

	for i := 0; i < n; i++ {
		paper := rankings[i]
		titleTrunc := paper.Title
		if len(titleTrunc) > 40 {
			titleTrunc = titleTrunc[:37] + "..."
		}

		fmt.Printf("%-4d | %.6f | %-9d | %-4d | %s\n",
			i+1, paper.Score, paper.Citations, paper.Year, titleTrunc)
	}
}

func CompareWithCitations(rankings []PaperScore, n int) {
	if n > len(rankings) {
		n = len(rankings)
	}

	// create citation-based ranking
	citationRankings := make([]PaperScore, len(rankings))
	copy(citationRankings, rankings)

	sort.Slice(citationRankings, func(i, j int) bool {
		return citationRankings[i].Score < citationRankings[j].Score
	})

	fmt.Printf("\nPageRank vs Citation Count (Top %d):\n", n)
	fmt.Println("PageRank Rank | Citation Rank | Paper ID    | PageRank | Citations")
	fmt.Println("--------------|---------------|-------------|----------|----------")

	// citation rank lookup
	citationRank := make(map[string]int)
	for i, paper := range citationRankings {
		citationRank[paper.PaperID] = i + 1
	}

	for i := 0; i < n; i++ {
		paper := rankings[i]
		cRank := citationRank[paper.PaperID]

		fmt.Printf("%-13d | %-13d | %-11s | %.6f | %d\n",
			i+1, cRank, paper.PaperID, paper.Score, paper.Citations)
	}
}
