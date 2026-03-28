package generate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/adtyap26/redmove/internal/format"
	"github.com/adtyap26/redmove/internal/pipeline"
	"github.com/brianvoe/gofakeit/v7"
)

// FieldSpec maps a record field name to a faker generator type.
type FieldSpec struct {
	Name      string
	FakerType string
}

// FakerOpts configures the FakerReader.
type FakerOpts struct {
	Count       int
	KeyTemplate string // "user:#{seq}"
	Fields      string // "name:name,email:email,age:number"
	RecordType  string // default "hash"
}

// FakerReader implements pipeline.Reader, generating synthetic records.
type FakerReader struct {
	count   int
	kt      *format.KeyTemplate
	specs   []FieldSpec
	recType string
}

// NewFakerReader validates options and creates a FakerReader.
func NewFakerReader(opts FakerOpts) (*FakerReader, error) {
	if opts.Count <= 0 {
		return nil, fmt.Errorf("count must be positive")
	}
	if opts.RecordType == "" {
		opts.RecordType = "hash"
	}

	kt, err := format.ParseKeyTemplate(opts.KeyTemplate)
	if err != nil {
		return nil, fmt.Errorf("invalid key template: %w", err)
	}

	specs, err := ParseFieldSpecs(opts.Fields)
	if err != nil {
		return nil, err
	}

	return &FakerReader{
		count:   opts.Count,
		kt:      kt,
		specs:   specs,
		recType: opts.RecordType,
	}, nil
}

// Read implements pipeline.Reader.
func (r *FakerReader) Read(ctx context.Context, out chan<- pipeline.Record) error {
	defer close(out)
	fake := gofakeit.New(0)

	for i := 1; i <= r.count; i++ {
		fields := make(map[string]any, len(r.specs)+1)
		fields["seq"] = i
		for _, spec := range r.specs {
			fields[spec.Name] = generateValue(fake, spec.FakerType)
		}

		key, err := r.kt.Execute(fields)
		if err != nil {
			return fmt.Errorf("generate key for record %d: %w", i, err)
		}

		// Remove seq — it's only for key templating, not a Redis field.
		delete(fields, "seq")

		rec := pipeline.Record{
			Key:    key,
			Type:   r.recType,
			TTL:    -1,
			Fields: fields,
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// ParseFieldSpecs parses "name:type,name:type,..." into a slice of FieldSpec.
func ParseFieldSpecs(spec string) ([]FieldSpec, error) {
	if spec == "" {
		return nil, fmt.Errorf("fields spec is required")
	}

	var specs []FieldSpec
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		colonIdx := strings.Index(part, ":")
		if colonIdx <= 0 {
			return nil, fmt.Errorf("invalid field spec %q: expected name:type", part)
		}
		name := strings.TrimSpace(part[:colonIdx])
		fakerType := strings.TrimSpace(part[colonIdx+1:])

		if !isValidFakerType(fakerType) {
			return nil, fmt.Errorf("unknown faker type %q (valid: name, email, phone, address, number, uuid, sentence, word, date, url, ipv4, bool)", fakerType)
		}

		specs = append(specs, FieldSpec{Name: name, FakerType: fakerType})
	}

	if len(specs) == 0 {
		return nil, fmt.Errorf("at least one field spec is required")
	}
	return specs, nil
}

var validFakerTypes = map[string]bool{
	"name": true, "email": true, "phone": true, "address": true,
	"number": true, "uuid": true, "sentence": true, "word": true,
	"date": true, "url": true, "ipv4": true, "bool": true,
}

func isValidFakerType(t string) bool {
	return validFakerTypes[t]
}

func generateValue(fake *gofakeit.Faker, fakerType string) any {
	switch fakerType {
	case "name":
		return fake.Name()
	case "email":
		return fake.Email()
	case "phone":
		return fake.Phone()
	case "address":
		return fake.Street() + ", " + fake.City() + ", " + fake.Country()
	case "number":
		return fake.Number(0, 999999)
	case "uuid":
		return fake.UUID()
	case "sentence":
		return fake.Sentence(6)
	case "word":
		return fake.Word()
	case "date":
		return fake.Date().Format(time.RFC3339)
	case "url":
		return fake.URL()
	case "ipv4":
		return fake.IPv4Address()
	case "bool":
		return fmt.Sprintf("%t", fake.Bool())
	default:
		return fake.Word()
	}
}
