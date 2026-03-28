package format

import (
	"context"
	"fmt"
	"strings"

	"github.com/adtyap26/redmove/internal/pipeline"
)

// KeyTemplate holds a parsed key template for field substitution.
type KeyTemplate struct {
	parts []templatePart
}

type templatePart struct {
	literal string
	field   string // non-empty if this part is a field reference
}

// ParseKeyTemplate parses a template like "user:#{id}:#{name}".
func ParseKeyTemplate(tmpl string) (*KeyTemplate, error) {
	var parts []templatePart
	remaining := tmpl

	for len(remaining) > 0 {
		idx := strings.Index(remaining, "#{")
		if idx == -1 {
			parts = append(parts, templatePart{literal: remaining})
			break
		}

		if idx > 0 {
			parts = append(parts, templatePart{literal: remaining[:idx]})
		}

		remaining = remaining[idx+2:]
		end := strings.Index(remaining, "}")
		if end == -1 {
			return nil, fmt.Errorf("unclosed #{ in key template")
		}

		field := remaining[:end]
		if field == "" {
			return nil, fmt.Errorf("empty field name in key template")
		}
		parts = append(parts, templatePart{field: field})
		remaining = remaining[end+1:]
	}

	hasField := false
	for _, p := range parts {
		if p.field != "" {
			hasField = true
			break
		}
	}
	if !hasField {
		return nil, fmt.Errorf("key template must contain at least one #{field} reference")
	}

	return &KeyTemplate{parts: parts}, nil
}

// Execute renders the template with the given fields.
func (t *KeyTemplate) Execute(fields map[string]any) (string, error) {
	var b strings.Builder
	for _, p := range t.parts {
		if p.field != "" {
			val, ok := fields[p.field]
			if !ok {
				return "", fmt.Errorf("field %q not found in record", p.field)
			}
			fmt.Fprintf(&b, "%v", val)
		} else {
			b.WriteString(p.literal)
		}
	}
	return b.String(), nil
}

// FileReaderOpts configures FileReader.
type FileReaderOpts struct {
	Path        string // file path
	Format      Format // auto-detected from extension if not explicitly set
	KeyTemplate string // e.g. "user:#{id}"
	RecordType  string // target Redis type: "hash", "string", etc.
}

// FileReader reads records from a file.
type FileReader struct {
	opts FileReaderOpts
	kt   *KeyTemplate
}

// NewFileReader creates a FileReader. Validates the key template at creation time.
func NewFileReader(opts FileReaderOpts) (*FileReader, error) {
	kt, err := ParseKeyTemplate(opts.KeyTemplate)
	if err != nil {
		return nil, fmt.Errorf("invalid key template: %w", err)
	}

	// Auto-detect format if not set.
	if opts.Format == 0 && opts.Path != "" {
		detected, err := DetectFormat(opts.Path)
		if err != nil {
			return nil, err
		}
		opts.Format = detected
	}

	if opts.RecordType == "" {
		opts.RecordType = "hash"
	}

	return &FileReader{opts: opts, kt: kt}, nil
}

// Read implements pipeline.Reader.
func (r *FileReader) Read(ctx context.Context, out chan<- pipeline.Record) error {
	defer close(out)

	switch r.opts.Format {
	case FormatCSV:
		return readCSV(ctx, r.opts.Path, r.kt, r.opts.RecordType, out)
	case FormatJSON:
		return readJSON(ctx, r.opts.Path, r.kt, r.opts.RecordType, out)
	case FormatJSONL:
		return readJSONL(ctx, r.opts.Path, r.kt, r.opts.RecordType, out)
	case FormatXML:
		return readXML(ctx, r.opts.Path, r.kt, r.opts.RecordType, out)
	default:
		return fmt.Errorf("unsupported format: %v", r.opts.Format)
	}
}
