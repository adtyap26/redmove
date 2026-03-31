package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// MigrateIndexes copies all RediSearch indexes from src to dst.
// It reads each index definition via FT.INFO and recreates it on the target.
// Returns the number of indexes successfully migrated.
func MigrateIndexes(ctx context.Context, src, dst goredis.UniversalClient) (int, error) {
	type doer interface {
		Do(ctx context.Context, args ...any) *goredis.Cmd
	}

	srcDoer, ok := src.(doer)
	if !ok {
		return 0, fmt.Errorf("source client does not support raw commands (FT._LIST)")
	}
	dstDoer, ok := dst.(doer)
	if !ok {
		return 0, fmt.Errorf("target client does not support raw commands (FT.CREATE)")
	}

	// List all indexes on source.
	listRes, err := srcDoer.Do(ctx, "FT._LIST").Result()
	if err != nil {
		return 0, fmt.Errorf("FT._LIST: %w", err)
	}

	indexNames, ok := listRes.([]any)
	if !ok {
		return 0, fmt.Errorf("unexpected FT._LIST result type: %T", listRes)
	}

	var migrated int
	for _, nameAny := range indexNames {
		name, ok := nameAny.(string)
		if !ok {
			continue
		}

		infoRes, err := srcDoer.Do(ctx, "FT.INFO", name).Result()
		if err != nil {
			slog.Warn("FT.INFO failed", "index", name, "error", err)
			continue
		}

		createArgs, err := buildFTCreate(name, infoRes)
		if err != nil {
			slog.Warn("failed to build FT.CREATE", "index", name, "error", err)
			continue
		}

		// Drop on target first (ignore error — may not exist).
		dstDoer.Do(ctx, "FT.DROPINDEX", name)

		if _, err := dstDoer.Do(ctx, createArgs...).Result(); err != nil {
			slog.Warn("FT.CREATE failed", "index", name, "error", err)
			continue
		}
		slog.Info("migrated index", "index", name)
		migrated++
	}

	return migrated, nil
}

// buildFTCreate reconstructs FT.CREATE args from the FT.INFO response.
//
// RediSearch has two field-list formats depending on version:
//   - New (2.x RESP3): each field is ["identifier","name","attribute","name","type","TEXT",...]
//   - Old (1.x / 2.x RESP2): each field is ["name","type","TEXT","WEIGHT","1",...]
//     where the first element is the field identifier directly.
func buildFTCreate(name string, infoRaw any) ([]any, error) {
	info, ok := infoRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected FT.INFO type: %T", infoRaw)
	}

	topMap := flatListToMap(info)

	args := []any{"FT.CREATE", name}

	// ON <type> PREFIX <n> <prefix...>
	if defRaw, ok := topMap["index_definition"]; ok {
		if def, ok := defRaw.([]any); ok {
			defMap := flatListToMap(def)
			if kt, ok := defMap["key_type"].(string); ok {
				args = append(args, "ON", strings.ToUpper(kt))
			}
			if prefRaw, ok := defMap["prefixes"]; ok {
				prefs := toStringSlice(prefRaw)
				if len(prefs) > 0 {
					args = append(args, "PREFIX", int64(len(prefs)))
					for _, p := range prefs {
						args = append(args, p)
					}
				}
			}
		}
	}

	// SCHEMA <fields...>
	// RediSearch 2.x uses "attributes"; older may use "fields".
	var fieldsSlice []any
	if f, ok := topMap["attributes"].([]any); ok {
		fieldsSlice = f
	} else if f, ok := topMap["fields"].([]any); ok {
		fieldsSlice = f
	}

	if len(fieldsSlice) == 0 {
		return nil, fmt.Errorf("index %q has no attributes in FT.INFO", name)
	}

	args = append(args, "SCHEMA")
	for _, fieldRaw := range fieldsSlice {
		field, ok := fieldRaw.([]any)
		if !ok || len(field) < 3 {
			continue
		}

		// Detect format: new format has "identifier" as first key.
		fm := flatListToMap(field)
		identifier, _ := fm["identifier"].(string)

		if identifier == "" {
			// Old format: first element is the field name; rest are key-value pairs.
			identifier, ok = field[0].(string)
			if !ok || identifier == "" {
				continue
			}
			fm = flatListToMap(field[1:])
		} else {
			// New format: may have "attribute" alias.
			if attr, ok := fm["attribute"].(string); ok && attr != "" && attr != identifier {
				// handled below
				_ = attr
			}
		}

		args = append(args, identifier)

		// Optional alias (new format only).
		if attr, ok := fm["attribute"].(string); ok && attr != "" && attr != identifier {
			args = append(args, "AS", attr)
		}

		fieldType, _ := fm["type"].(string)
		if fieldType == "" {
			continue
		}
		fieldTypeUpper := strings.ToUpper(fieldType)

		if fieldTypeUpper == "VECTOR" {
			slog.Warn("VECTOR field skipped in index recreation (complex params)", "index", name, "field", identifier)
			args = args[:len(args)-1] // remove identifier we just added
			continue
		}

		args = append(args, fieldTypeUpper)

		// Type-specific options.
		switch fieldTypeUpper {
		case "TEXT":
			if w, ok := fm["WEIGHT"].(string); ok && w != "" && w != "1" {
				args = append(args, "WEIGHT", w)
			}
		case "TAG":
			if sep, ok := fm["SEPARATOR"].(string); ok && sep != "" {
				args = append(args, "SEPARATOR", sep)
			}
		}

		// Flags: SORTABLE, NOSTEM, NOINDEX, UNF, CASESENSITIVE, etc.
		// New format stores them in a nested "flags" array.
		if flags, ok := fm["flags"].([]any); ok {
			for _, f := range flags {
				if flag, ok := f.(string); ok && flag != "" {
					args = append(args, strings.ToUpper(flag))
				}
			}
		}
	}

	return args, nil
}

// toStringSlice converts a value that may be a []any or a plain string into []string.
func toStringSlice(v any) []string {
	switch val := v.(type) {
	case string:
		if val == "" {
			return nil
		}
		return []string{val}
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// flatListToMap converts a flat alternating [key, value, key, value...] slice
// into a map. Values may themselves be slices (nested).
func flatListToMap(data []any) map[string]any {
	m := make(map[string]any, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		k, ok := data[i].(string)
		if !ok {
			continue
		}
		m[k] = data[i+1]
	}
	return m
}
