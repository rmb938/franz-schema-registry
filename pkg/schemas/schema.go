package schemas

import (
	"fmt"
	"io"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeJSON     SchemaType = "JSON"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

type ParsedSchema interface {
	IsBackwardsCompatible(previousSchema ParsedSchema) (bool, error)
}

func ParseSchema(rawSchema string, schemaType SchemaType, rawReferences []string, rawReferenceNames []string) (ParsedSchema, error) {
	var parsedSchema ParsedSchema

	switch schemaType {
	case SchemaTypeAvro:
		avroCache := &avro.SchemaCache{}

		references := make(map[string]avro.Schema)
		for _, reference := range rawReferences {
			schema, err := avro.ParseWithCache(reference, "", avroCache)
			if err != nil {
				return nil, fmt.Errorf("error parsing avro schema reference %s: %w", reference, err)
			}

			// only add to references if it's a named schema
			// if it isn't there is nothing to overwrite
			if namedSchema, ok := schema.(avro.NamedSchema); ok {
				references[namedSchema.FullName()] = schema
			}
		}

		avroSchema, err := avro.ParseWithCache(rawSchema, "", avroCache)
		if err != nil {
			return nil, fmt.Errorf("error parsing avro schema: %w", err)
		}

		// so make sure it isn't overwriting any references
		if name, ok := isAvroOverrideReferenceName(references, avroSchema, nil); ok {
			return nil, fmt.Errorf("can't redefine: %s", name)
		}

		parsedSchema = &ParsedAvroSchema{
			avroSchema: avroSchema,
		}
		break
	case SchemaTypeJSON:
		compiler := jsonschema.NewCompiler()

		// custom loader because we don't want to read from files or URLs
		// first we don't want anyone reading from our filesystem, this is just not good
		// second if there is a RCE bug then allowing loading from files or URLs is a very bad idea
		// confluent sr currently allows loading from files or URLs but this seems like a security hole
		// I'm purposefully breaking compatibility with confluent sr to not allow this and to fix the security hole
		// If someone wants to allow loading from a known safe URL we probably could create a configuration
		// that allows loading from refs that match a certain regex pattern
		compiler.LoadURL = func(s string) (io.ReadCloser, error) {
			return nil, fmt.Errorf("$ref %s not found", s)
		}

		for index, reference := range rawReferences {
			err := compiler.AddResource(rawReferenceNames[index], strings.NewReader(reference))
			if err != nil {
				return nil, fmt.Errorf("error parsing json schema reference: %w", err)
			}

			_, err = compiler.Compile(rawReferenceNames[index])
			if err != nil {
				return nil, fmt.Errorf("error compiling json schema reference: %w", err)
			}
		}

		err := compiler.AddResource("schema.json", strings.NewReader(rawSchema))
		if err != nil {
			return nil, fmt.Errorf("error parsing json schema: %w", err)
		}

		jsonSchema, err := compiler.Compile("schema.json")
		if err != nil {
			return nil, fmt.Errorf("error compiling json schema: %w", err)
		}

		// TODO: do we need to do the same overwriting references check as avro?
		//  maybe unique $id's?

		parsedSchema = &ParsedJSONSchema{
			jsonSchema: jsonSchema,
		}

		break
	default:
		return nil, fmt.Errorf("unknown schema type: %s", schemaType)
	}

	return parsedSchema, nil
}
