package data

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

// metadata of each paper
type Paper struct {
	ID                string    `json:"id"`
	Title             string    `json:"title"`
	Authors           []string  `json:"authors"`
	Year              int       `json:"year"`
	Abstract          string    `json:"abstract"`
	Publisher         string    `json:"publisher"`
	BookTitle         string    `json:"booktitle"`
	DOI               string    `json:"doi"`
	URL               string    `json:"url"`
	NumCitedBy        int       `json:"num_cited_by"`
	Citations         []string  `json:"citations"`
	CorpusPaperID     int64     `json:"-"`
	AbstractEmbedding []float32 `json:"abstract_embedding,omitempty"`
}

type CitationEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// parsing statistics
type ParseStats struct {
	TotalPapers    int `json:"total_papers"`
	TotalCitations int `json:"total_citations"`
	YearRange      struct {
		Min int `json:"min_year"`
		Max int `json:"max_year"`
	} `json:"year_range"`
}

// Accumulation of all data
type ParsedData struct {
	Papers    []Paper        `json:"papers"`
	Citations []CitationEdge `json:"citations"`
	Stats     ParseStats     `json:"stats"`
}

func ParseACLData(papersPath, citationsPath string, maxPapers int) (*ParsedData, error) {
	fmt.Println("--- Starting Paper Parsing ---")
	papers, stats, err := parsePapersParquet(papersPath, maxPapers)
	if err != nil {
		return nil, fmt.Errorf("failed to parse papers: %v", err)
	}

	// build a map to link the corpus_id to the acl_id
	corpusToACL := make(map[int64]string)
	for _, paper := range papers {
		if paper.CorpusPaperID != 0 && paper.ID != "" {
			corpusToACL[paper.CorpusPaperID] = paper.ID
		}
	}

	citations, err := parseCitationsParquet(citationsPath, corpusToACL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse citations: %v", err)
	}

	stats.TotalCitations = len(citations)

	updatePaperCitations(papers, citations)

	return &ParsedData{
		Papers:    papers,
		Citations: citations,
		Stats:     *stats,
	}, nil
}

func parsePapersParquet(parquetPath string, maxPapers int) ([]Paper, *ParseStats, error) {

	f, err := os.Open(parquetPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open parquet file: %v", err)
	}
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parquet reader: %v", err)
	}

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create arrow reader: %v", err)
	}

	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read table: %v", err)
	}
	defer table.Release()

	numRows := int(table.NumRows())
	if maxPapers > 0 && maxPapers < numRows {
		numRows = maxPapers
	}

	fmt.Printf("Parquet file contains %d rows. Processing %d.\n", table.NumRows(), numRows)

	papers := make([]Paper, 0, numRows)
	stats := &ParseStats{}
	minYear, maxYear := 9999, 0

	columnMap := make(map[string]int)
	for i, field := range table.Schema().Fields() {
		columnMap[field.Name] = i
	}

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		paper := Paper{}
		for colName, colIdx := range columnMap {
			column := table.Column(colIdx)

			switch colName {
			case "acl_id":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.ID = val
				}
			case "title":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.Title = val
				}
			case "author":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.Authors = parseAuthors(val)
				}
			case "year":
				if val, err := getInt64ValueFromColumn(column, rowIdx); err == nil && val > 1900 && val < 2030 {
					paper.Year = int(val)
					if paper.Year < minYear {
						minYear = paper.Year
					}
					if paper.Year > maxYear {
						maxYear = paper.Year
					}
				}
			case "abstract":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.Abstract = val
				}
			case "publisher":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.Publisher = val
				}
			case "booktitle":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.BookTitle = val
				}
			case "doi":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.DOI = val
				}
			case "url":
				if val, err := getStringValueFromColumn(column, rowIdx); err == nil {
					paper.URL = val
				}
			case "numcitedby":
				if val, err := getInt64ValueFromColumn(column, rowIdx); err == nil {
					paper.NumCitedBy = int(val)
				}
			case "corpus_paper_id":
				if val, err := getInt64ValueFromColumn(column, rowIdx); err == nil {
					paper.CorpusPaperID = val
				}
			}
		}

		if paper.ID == "" || paper.Title == "" {
			continue
		}
		papers = append(papers, paper)
	}

	stats.TotalPapers = len(papers)
	if minYear != 9999 {
		stats.YearRange.Min = minYear
		stats.YearRange.Max = maxYear
	}

	fmt.Printf("Successfully parsed %d papers.\n", len(papers))
	return papers, stats, nil
}

func parseCitationsParquet(filePath string, corpusToACL map[int64]string) ([]CitationEdge, error) {
	fmt.Printf("Opening citations parquet file: %s\n", filePath)

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open citations parquet file: %v", err)
	}
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader for citations: %v", err)
	}

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader for citations: %v", err)
	}

	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to read citations table: %v", err)
	}
	defer table.Release()

	fmt.Printf("Citations file contains %d rows.\n", table.NumRows())

	var citations []CitationEdge
	skippedCitations := 0

	colMap := make(map[string]int)
	for i, field := range table.Schema().Fields() {
		colMap[field.Name] = i
	}

	citingIDCol := table.Column(colMap["citingpaperid"])
	citedIDCol := table.Column(colMap["citedpaperid"])
	isCitingACLCol := table.Column(colMap["is_citingpaperid_acl"])
	isCitedACLCol := table.Column(colMap["is_citedpaperid_acl"])

	for r := 0; r < int(table.NumRows()); r++ {
		isCitingACL, err1 := getBoolValueFromColumn(isCitingACLCol, r)
		isCitedACL, err2 := getBoolValueFromColumn(isCitedACLCol, r)
		if err1 != nil || err2 != nil || !isCitingACL || !isCitedACL {
			skippedCitations++
			continue
		}

		citingID, err1 := getInt64ValueFromColumn(citingIDCol, r)
		citedID, err2 := getInt64ValueFromColumn(citedIDCol, r)
		if err1 != nil || err2 != nil {
			skippedCitations++
			continue
		}

		fromACLId, fromExists := corpusToACL[citingID]
		toACLId, toExists := corpusToACL[citedID]

		if !fromExists || !toExists || fromACLId == toACLId {
			skippedCitations++
			continue
		}

		citations = append(citations, CitationEdge{From: fromACLId, To: toACLId})
	}

	fmt.Printf("Successfully parsed %d valid citations (skipped %d).\n", len(citations), skippedCitations)
	return citations, nil
}

func findChunk(column *arrow.Column, rowIdx int) (chunk arrow.Array, localIndex int, err error) {
	chunkIdx := 0
	localRowIdx := rowIdx

	// Find which chunk contains our row
	for chunkIdx < column.Data().Len() {
		chunk = column.Data().Chunk(chunkIdx)
		if localRowIdx < chunk.Len() {
			return chunk, localRowIdx, nil
		}
		localRowIdx -= chunk.Len()
		chunkIdx++
	}

	return nil, 0, fmt.Errorf("row index %d out of bounds for column with %d rows", rowIdx, column.Len())
}

func getStringValueFromColumn(column *arrow.Column, rowIdx int) (string, error) {
	chunk, localIdx, err := findChunk(column, rowIdx)
	if err != nil {
		return "", err
	}
	if chunk.IsNull(localIdx) {
		return "", fmt.Errorf("value is null")
	}

	switch arr := chunk.(type) {
	case *array.String:
		return arr.Value(localIdx), nil
	case *array.Binary:
		return string(arr.Value(localIdx)), nil
	default:
		return "", fmt.Errorf("column is not a string/binary type")
	}
}

func getInt64ValueFromColumn(column *arrow.Column, rowIdx int) (int64, error) {
	chunk, localIdx, err := findChunk(column, rowIdx)
	if err != nil {
		return 0, err
	}
	if chunk.IsNull(localIdx) {
		return 0, fmt.Errorf("value is null")
	}

	switch arr := chunk.(type) {
	case *array.Int32:
		return int64(arr.Value(localIdx)), nil
	case *array.Int64:
		return arr.Value(localIdx), nil
	default:
		return 0, fmt.Errorf("column is not an integer type")
	}
}

func getBoolValueFromColumn(column *arrow.Column, rowIdx int) (bool, error) {
	chunk, localIdx, err := findChunk(column, rowIdx)
	if err != nil {
		return false, err
	}
	if chunk.IsNull(localIdx) {
		return false, fmt.Errorf("value is null")
	}

	if arr, ok := chunk.(*array.Boolean); ok {
		return arr.Value(localIdx), nil
	}
	return false, fmt.Errorf("column is not a boolean type")
}

func parseAuthors(authorStr string) []string {
	if authorStr == "" {
		return []string{}
	}
	separators := []string{";", ",", " and ", " & "}
	var authors []string
	for _, sep := range separators {
		if strings.Contains(authorStr, sep) {
			authors = strings.Split(authorStr, sep)
			break
		}
	}
	if len(authors) == 0 {
		authors = []string{authorStr}
	}

	cleanedAuthors := make([]string, 0, len(authors))
	for _, author := range authors {
		cleaned := strings.TrimSpace(author)
		if cleaned != "" && len(cleaned) > 1 {
			cleanedAuthors = append(cleanedAuthors, cleaned)
		}
	}
	return cleanedAuthors
}

func updatePaperCitations(papers []Paper, citations []CitationEdge) {
	citationMap := make(map[string][]string)
	for _, citation := range citations {
		citationMap[citation.From] = append(citationMap[citation.From], citation.To)
	}
	for i := range papers {
		paper := &papers[i]
		if citedPapers, exists := citationMap[paper.ID]; exists {
			paper.Citations = citedPapers
		} else {
			paper.Citations = []string{}
		}
	}
}

func SaveParsedData(data *ParsedData, outputPath string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %v", err)
	}
	return os.WriteFile(outputPath, jsonData, 0644)
}

func LoadParsedData(inputPath string) (*ParsedData, error) {
	jsonData, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file: %v", err)
	}
	var data ParsedData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON data: %v", err)
	}
	return &data, nil
}

func PrintParsingStats(stats ParseStats) {
	fmt.Println("\n=== Parsing Statistics ===")
	fmt.Printf("Total papers: %d\n", stats.TotalPapers)
	fmt.Printf("Total citations: %d\n", stats.TotalCitations)
	fmt.Printf("Year range: %d - %d\n", stats.YearRange.Min, stats.YearRange.Max)
	if stats.TotalPapers > 0 {
		avgCitations := float64(stats.TotalCitations) / float64(stats.TotalPapers)
		fmt.Printf("Average citations per paper: %.2f\n", avgCitations)
	}
	fmt.Println("========================")
}
