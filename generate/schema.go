package main

import (
	"fmt"
	"log"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

func convertStruct(entries map[string]string, schema *openapi3.Schema) {
	for name, prop := range schema.Properties {
		entries[name] = convertType(prop.Value)
	}
	for _, ref := range schema.AnyOf {
		convertStruct(entries, ref.Value)
	}
	for _, ref := range schema.AllOf {
		convertStruct(entries, ref.Value)
	}
	for _, ref := range schema.OneOf {
		convertStruct(entries, ref.Value)
	}
}

func convertType(schema *openapi3.Schema) string {
	switch {
	case schema.Type.Is("integer"):
		switch schema.Format {
		case "int32":
			return `"INT32"`
		case "int64":
			return `"INT64"`
		default:
			return `"INT64"`
		}
	case schema.Type.Is("number"):
		return `"DOUBLE"`
	case schema.Type.Is("string"):
		switch schema.Format {
		case "date":
			return `"DATE"`
		case "date-time":
			return `"DATETIME"`
		case "byte", "binary":
			return `"BINARY"`
		default:
			return `"STRING"`
		}
	case schema.Type.Is("boolean"):
		return `"BOOLEAN"`
	case schema.Type.Is("array"):
		return "[" + convertType(schema.Items.Value) + "]"
	case schema.Type.Is("object"):
		if schema.AdditionalProperties.Has != nil && *schema.AdditionalProperties.Has {
			return `"MAP(STRING,STRING)"`
		}
		entries := make(map[string]string)
		convertStruct(entries, schema)
		names := slices.Collect(maps.Keys(entries))
		slices.Sort(names)
		var sb strings.Builder
		sb.WriteRune('{')
		for idx, name := range names {
			if idx > 0 {
				sb.WriteString(",")
			}
			sb.WriteRune('"')
			sb.WriteString(name)
			sb.WriteRune('"')
			sb.WriteRune(':')
			sb.WriteString(entries[name])
		}
		sb.WriteRune('}')
		return sb.String()
	default:
		return `"JSON"`
	}
}

func main() {
	root, err := openapi3.NewLoader().LoadFromURI(&url.URL{
		Scheme: "https",
		Host:   "github.com",
		Path:   "/github/rest-api-description/raw/refs/heads/main/descriptions/ghec/ghec.yaml",
	})
	if err != nil {
		log.Fatalf("(*openapi3.Loader).LoadFromURI() failed: %v", err)
	}
	fmt.Println("// go run -C generate . > src/generated_types.cpp")
	for name, schema := range root.Components.Schemas {
		fmt.Printf("{%q,%q},\n", name, convertType(schema.Value))
	}
}
