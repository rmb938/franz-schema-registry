package schemas

import (
	"fmt"

	"github.com/hamba/avro/v2"
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

func ParseSchema(rawSchema string, schemaType SchemaType, rawReferences []string) (ParsedSchema, error) {
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
	default:
		return nil, fmt.Errorf("unknown schema type: %s", schemaType)
	}

	return parsedSchema, nil
}

func isAvroOverrideReferenceName(references map[string]avro.Schema, schema avro.Schema, seenRecords map[string]avro.Schema) (string, bool) {
	if seenRecords == nil {
		seenRecords = make(map[string]avro.Schema)
	}

	switch v := schema.(type) {
	// named schemas
	case avro.NamedSchema:
		schemaName := v.FullName()
		// schema is in the references
		if ref, ok := references[schemaName]; ok {
			// but schema doesn't match reference so we've duplicated
			if ref != v {
				return schemaName, true
			}
		}

		// schema was seen before
		if ref, ok := seenRecords[schemaName]; ok {
			// seen schema is not the same so we've duplicated
			if ref != v {
				return schemaName, true
			}

			// schema is the same, so we don't need to go down the chain again
			return "", false
		} else {
			// schema was not seem before so add to map
			seenRecords[schemaName] = v
		}
		switch namedSchema := v.(type) {
		// record schema so recurse it's fields
		case *avro.RecordSchema:
			for _, field := range namedSchema.Fields() {
				return isAvroOverrideReferenceName(references, field.Type(), seenRecords)
			}

		// named schemas that can't recurse
		case *avro.EnumSchema:
			return "", false
		case *avro.FixedSchema:
			return "", false
		}
		break
	// collection schemas so recurse their items
	case *avro.RefSchema:
		return isAvroOverrideReferenceName(references, v.Schema(), seenRecords)
	case *avro.ArraySchema:
		return isAvroOverrideReferenceName(references, v.Items(), seenRecords)
	case *avro.MapSchema:
		return isAvroOverrideReferenceName(references, v.Values(), seenRecords)

	// everything else
	default:
		return "", false
	}

	return "", false
}
