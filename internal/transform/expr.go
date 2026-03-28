package transform

import (
	"context"
	"fmt"
	"strings"

	"github.com/adtyap26/redmove/internal/pipeline"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// TransformOpts configures the expression processor.
type TransformOpts struct {
	Transform string // "name=upper(name), score=score*2"
	Filter    string // "type == 'hash' && score > 100"
}

// fieldExpr is one parsed field=expression assignment.
type fieldExpr struct {
	field   string
	program *vm.Program
}

// ExprProcessor implements pipeline.Processor with expr-lang transforms and filters.
type ExprProcessor struct {
	transforms []fieldExpr
	filter     *vm.Program // nil if no filter
}

// NewExprProcessor parses and compiles expressions.
func NewExprProcessor(opts TransformOpts) (*ExprProcessor, error) {
	p := &ExprProcessor{}

	if opts.Filter != "" {
		prog, err := expr.Compile(opts.Filter, expr.AsBool())
		if err != nil {
			return nil, fmt.Errorf("compile filter %q: %w", opts.Filter, err)
		}
		p.filter = prog
	}

	if opts.Transform != "" {
		parts := splitTransforms(opts.Transform)
		for _, part := range parts {
			eqIdx := strings.Index(part, "=")
			if eqIdx <= 0 {
				return nil, fmt.Errorf("invalid transform %q: expected field=expression", part)
			}
			field := strings.TrimSpace(part[:eqIdx])
			expression := strings.TrimSpace(part[eqIdx+1:])
			if field == "" || expression == "" {
				return nil, fmt.Errorf("invalid transform %q: empty field or expression", part)
			}
			prog, err := expr.Compile(expression)
			if err != nil {
				return nil, fmt.Errorf("compile transform %q: %w", expression, err)
			}
			p.transforms = append(p.transforms, fieldExpr{field: field, program: prog})
		}
	}

	return p, nil
}

// Process implements pipeline.Processor.
func (p *ExprProcessor) Process(ctx context.Context, in <-chan pipeline.Record, out chan<- pipeline.Record) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, ok := <-in:
			if !ok {
				return nil
			}

			env := buildEnv(rec)

			// Apply filter.
			if p.filter != nil {
				result, err := expr.Run(p.filter, env)
				if err != nil {
					continue // skip records that fail filter evaluation
				}
				if pass, ok := result.(bool); !ok || !pass {
					continue
				}
			}

			// Apply transforms.
			for _, t := range p.transforms {
				result, err := expr.Run(t.program, env)
				if err != nil {
					continue // skip this transform on error
				}
				rec.Fields[t.field] = result
				env[t.field] = result // update env for chained transforms
			}

			select {
			case out <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// buildEnv creates the expression environment from a Record.
func buildEnv(rec pipeline.Record) map[string]any {
	env := make(map[string]any, len(rec.Fields)+3)
	for k, v := range rec.Fields {
		env[k] = v
	}
	env["key"] = rec.Key
	env["type"] = rec.Type
	env["ttl"] = rec.TTL.Seconds()
	return env
}

// splitTransforms splits "a=expr1, b=expr2" respecting parentheses.
func splitTransforms(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				part := strings.TrimSpace(s[start:i])
				if part != "" {
					parts = append(parts, part)
				}
				start = i + 1
			}
		}
	}
	if last := strings.TrimSpace(s[start:]); last != "" {
		parts = append(parts, last)
	}
	return parts
}
