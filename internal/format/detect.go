package format

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Format represents a supported file format.
type Format int

const (
	FormatCSV  Format = iota
	FormatJSON
	FormatJSONL
	FormatXML
)

func (f Format) String() string {
	switch f {
	case FormatCSV:
		return "csv"
	case FormatJSON:
		return "json"
	case FormatJSONL:
		return "jsonl"
	case FormatXML:
		return "xml"
	default:
		return "unknown"
	}
}

// DetectFormat returns the format based on file extension.
func DetectFormat(filename string) (Format, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".csv":
		return FormatCSV, nil
	case ".json":
		return FormatJSON, nil
	case ".jsonl", ".ndjson":
		return FormatJSONL, nil
	case ".xml":
		return FormatXML, nil
	default:
		return 0, fmt.Errorf("unsupported file extension: %q (use .csv, .json, .jsonl, or .xml)", ext)
	}
}

// ParseFormat parses a format string.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(s) {
	case "csv":
		return FormatCSV, nil
	case "json":
		return FormatJSON, nil
	case "jsonl", "ndjson":
		return FormatJSONL, nil
	case "xml":
		return FormatXML, nil
	default:
		return 0, fmt.Errorf("unsupported format: %q (use csv, json, jsonl, or xml)", s)
	}
}
