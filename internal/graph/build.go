package graph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"paper-rank/internal/data"
)

type Graph struct {
	Nodes     []Node              `json:"nodes"`
	Edges     []Edge              `json:"edges"`
	AdjList   map[string][]string `json:"adj_list"`   // paper_id -> list of cited paper_ids
	InDegree  map[string]int      `json:"in_degree"`  // paper_id -> number of papers citing it
	OutDegree map[string]int      `json:"out_degree"` // paper_id -> number of papers it cites
	Stats     GraphStats          `json:"stats"`
}

type Node struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Year    int      `json:"year"`
	Authors []string `json:"authors"`
}

type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type PaperInfo struct {
	Node         Node     `json:"node"`
	InDegree     int      `json:"in_degree"`
	OutDegree    int      `json:"out_degree"`
	CitedPapers  []string `json:"cited_papers"`  // Papers this paper cites
	CitingPapers []string `json:"citing_papers"` // Papers that cite this paper
}

type PaperRanking struct {
	PaperID    string   `json:"paper_id"`
	Title      string   `json:"title"`
	Year       int      `json:"year"`
	Authors    []string `json:"authors"`
	Citations  int      `json:"citations"`  // In-degree (how many cite this paper)
	References int      `json:"references"` // Out-degree (how many this paper cites)
}

type GraphStats struct {
	TotalNodes      int     `json:"total_nodes"`
	TotalEdges      int     `json:"total_edges"`
	AvgInDegree     float64 `json:"avg_in_degree"`
	AvgOutDegree    float64 `json:"avg_out_degree"`
	MaxInDegree     int     `json:"max_in_degree"`
	MaxOutDegree    int     `json:"max_out_degree"`
	MostCitedPaper  string  `json:"most_cited_paper"`
	MostCitingPaper string  `json:"most_citing_paper"`
	IsolatedNodes   int     `json:"isolated_nodes"` // nodes with no edges
	SelfCitations   int     `json:"self_citations"` // node pointing to itself
	GraphDensity    float64 `json:"graph_density"`  // edges/possible_edges
}

func BuildGraph(parsedDataPath string) (*Graph, error) {
	fmt.Printf("Loading parsed data from: %s\n", parsedDataPath)

	parsedData, err := data.LoadParsedData(parsedDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load parsed data: %v", err)
	}

	fmt.Printf("Building graph from %d papers and %d citations...\n",
		len(parsedData.Papers), len(parsedData.Citations))

	graph := &Graph{
		Nodes:     make([]Node, 0, len(parsedData.Papers)),
		Edges:     make([]Edge, 0, len(parsedData.Citations)),
		AdjList:   make(map[string][]string),
		InDegree:  make(map[string]int),
		OutDegree: make(map[string]int),
	}

	for _, paper := range parsedData.Papers {
		node := Node{
			ID:      paper.ID,
			Title:   paper.Title,
			Year:    paper.Year,
			Authors: paper.Authors,
		}
		graph.Nodes = append(graph.Nodes, node)

		graph.InDegree[paper.ID] = 0
		graph.OutDegree[paper.ID] = 0
		graph.AdjList[paper.ID] = []string{}
	}

	validEdges := 0
	selfCitations := 0

	for _, citation := range parsedData.Citations {
		_, fromExists := graph.InDegree[citation.From]
		_, toExists := graph.InDegree[citation.To]

		if !fromExists || !toExists {
			continue // skip citations to papers not in our dataset
		}

		// check for self-citations
		if citation.From == citation.To {
			selfCitations++
			continue
		}

		edge := Edge{
			From: citation.From,
			To:   citation.To,
		}
		graph.Edges = append(graph.Edges, edge)

		graph.AdjList[citation.From] = append(graph.AdjList[citation.From], citation.To)

		graph.OutDegree[citation.From]++
		graph.InDegree[citation.To]++

		validEdges++
	}

	fmt.Printf("Created %d valid edges (filtered out %d self-citations)\n",
		validEdges, selfCitations)

	graph.Stats = calculateGraphStats(graph, selfCitations)

	return graph, nil
}

func calculateGraphStats(graph *Graph, selfCitations int) GraphStats {
	stats := GraphStats{
		TotalNodes:    len(graph.Nodes),
		TotalEdges:    len(graph.Edges),
		SelfCitations: selfCitations,
	}

	if stats.TotalNodes == 0 {
		return stats
	}

	var totalInDegree, totalOutDegree int
	var maxInDegree, maxOutDegree int
	var mostCitedPaper, mostCitingPaper string
	var isolatedNodes int

	for paperID, inDegree := range graph.InDegree {
		outDegree := graph.OutDegree[paperID]

		totalInDegree += inDegree
		totalOutDegree += outDegree

		if inDegree > maxInDegree {
			maxInDegree = inDegree
			mostCitedPaper = paperID
		}

		if outDegree > maxOutDegree {
			maxOutDegree = outDegree
			mostCitingPaper = paperID
		}

		if inDegree == 0 && outDegree == 0 {
			isolatedNodes++
		}
	}

	stats.AvgInDegree = float64(totalInDegree) / float64(stats.TotalNodes)
	stats.AvgOutDegree = float64(totalOutDegree) / float64(stats.TotalNodes)
	stats.MaxInDegree = maxInDegree
	stats.MaxOutDegree = maxOutDegree
	stats.MostCitedPaper = mostCitedPaper
	stats.MostCitingPaper = mostCitingPaper
	stats.IsolatedNodes = isolatedNodes

	maxPossibleEdges := stats.TotalNodes * (stats.TotalNodes - 1)
	if maxPossibleEdges > 0 {
		stats.GraphDensity = float64(stats.TotalEdges) / float64(maxPossibleEdges)
	}

	return stats
}

func SaveGraph(graph *Graph, outputPath string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	jsonData, err := json.MarshalIndent(graph, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal graph to JSON: %v", err)
	}

	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write graph file: %v", err)
	}

	return nil
}

func LoadGraph(inputPath string) (*Graph, error) {
	jsonData, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read graph file: %v", err)
	}

	var graph Graph
	if err := json.Unmarshal(jsonData, &graph); err != nil {
		return nil, fmt.Errorf("failed to unmarshal graph data: %v", err)
	}

	return &graph, nil
}

func PrintGraphStats(stats GraphStats) {
	fmt.Println("\n=== Graph Statistics ===")
	fmt.Printf("Total nodes (papers): %d\n", stats.TotalNodes)
	fmt.Printf("Total edges (citations): %d\n", stats.TotalEdges)
	fmt.Printf("Graph density: %.6f\n", stats.GraphDensity)
	fmt.Println()

	fmt.Printf("Average in-degree: %.2f\n", stats.AvgInDegree)
	fmt.Printf("Average out-degree: %.2f\n", stats.AvgOutDegree)
	fmt.Printf("Max in-degree: %d (paper: %s)\n", stats.MaxInDegree, stats.MostCitedPaper)
	fmt.Printf("Max out-degree: %d (paper: %s)\n", stats.MaxOutDegree, stats.MostCitingPaper)
	fmt.Println()

	fmt.Printf("Isolated nodes: %d (%.1f%%)\n",
		stats.IsolatedNodes,
		float64(stats.IsolatedNodes)/float64(stats.TotalNodes)*100)
	fmt.Printf("Self-citations found: %d (filtered out)\n", stats.SelfCitations)
}

func (g *Graph) GetMostCitedPapers(n int) []PaperRanking {
	rankings := make([]PaperRanking, 0, len(g.Nodes))

	for _, node := range g.Nodes {
		ranking := PaperRanking{
			PaperID:    node.ID,
			Title:      node.Title,
			Year:       node.Year,
			Authors:    node.Authors,
			Citations:  g.InDegree[node.ID],
			References: g.OutDegree[node.ID],
		}
		rankings = append(rankings, ranking)
	}

	for i := 0; i < len(rankings)-1; i++ {
		for j := i + 1; j < len(rankings); j++ {
			if rankings[j].Citations > rankings[i].Citations {
				rankings[i], rankings[j] = rankings[j], rankings[i]
			}
		}
	}

	if n > len(rankings) {
		n = len(rankings)
	}

	return rankings[:n]
}
