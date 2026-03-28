package format

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"

	"github.com/adtyap26/redmove/internal/pipeline"
)

// xmlRecord is the XML representation of a single record.
type xmlRecord struct {
	XMLName xml.Name   `xml:"record"`
	Key     string     `xml:"key,attr"`
	Type    string     `xml:"type,attr"`
	Fields  []xmlField `xml:"field"`
}

type xmlField struct {
	XMLName xml.Name `xml:"field"`
	Name    string   `xml:"name,attr"`
	Value   string   `xml:",chardata"`
}

// xmlRecords is the root element wrapper.
type xmlRecords struct {
	XMLName xml.Name    `xml:"records"`
	Records []xmlRecord `xml:"record"`
}

func readXML(ctx context.Context, path string, kt *KeyTemplate, recType string, out chan<- pipeline.Record) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open xml: %w", err)
	}
	defer f.Close()

	decoder := xml.NewDecoder(f)

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("decode xml: %w", err)
		}

		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "record" {
			continue
		}

		var xr xmlRecord
		if err := decoder.DecodeElement(&xr, &se); err != nil {
			return fmt.Errorf("decode xml record: %w", err)
		}

		fields := make(map[string]any, len(xr.Fields))
		for _, f := range xr.Fields {
			fields[f.Name] = f.Value
		}

		key, err := kt.Execute(fields)
		if err != nil {
			return fmt.Errorf("generate key: %w", err)
		}

		rec := pipeline.Record{
			Key:    key,
			Type:   recType,
			TTL:    -1,
			Fields: fields,
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func writeXML(ctx context.Context, w io.Writer, in <-chan pipeline.Record) (int64, error) {
	if _, err := io.WriteString(w, xml.Header); err != nil {
		return 0, err
	}
	if _, err := io.WriteString(w, "<records>\n"); err != nil {
		return 0, err
	}

	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")

	var written int64
	for {
		select {
		case rec, ok := <-in:
			if !ok {
				if _, err := io.WriteString(w, "</records>\n"); err != nil {
					return written, err
				}
				return written, nil
			}

			xr := xmlRecord{
				Key:  rec.Key,
				Type: rec.Type,
			}
			for k, v := range rec.Fields {
				xr.Fields = append(xr.Fields, xmlField{
					Name:  k,
					Value: fmt.Sprintf("%v", v),
				})
			}
			if err := enc.Encode(xr); err != nil {
				return written, err
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				return written, err
			}
			written++

		case <-ctx.Done():
			return written, ctx.Err()
		}
	}
}
