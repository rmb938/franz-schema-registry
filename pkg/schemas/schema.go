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

		// schema is a record
		if recordSchema, ok := avroSchema.(*avro.RecordSchema); ok {
			// so make sure it isn't overwriting any references
			if name, ok := isAvroOverrideReferenceName(references, recordSchema, nil); ok {
				return nil, fmt.Errorf("can't redefine: %s", name)
			}
		}

		parsedSchema = &ParsedAvroSchema{
			avroSchema: avroSchema,
		}
	default:
		return nil, fmt.Errorf("unknown schema type: %s", schemaType)
	}

	return parsedSchema, nil
}

func isAvroOverrideReferenceName(references map[string]avro.Schema, recordSchema *avro.RecordSchema, seenRecords map[string]avro.Schema) (string, bool) {
	if seenRecords == nil {
		seenRecords = make(map[string]avro.Schema)
		seenRecords[recordSchema.FullName()] = recordSchema
	}

	// get all the record fields
	for _, field := range recordSchema.Fields() {

		// field is a reference
		if refSchema, ok := field.Type().(*avro.RefSchema); ok {

			// to a record
			if subRecord, ok := refSchema.Schema().(*avro.RecordSchema); ok {
				recordName := subRecord.FullName()

				// record name is in the references
				if ref, ok := references[recordName]; ok {
					// record is not the reference so we are duplicated
					if ref != subRecord {
						// return normal name since it's overwriting in the same namespace
						return subRecord.Name(), true
					}

					// record is the reference so continue to new field
					continue
				} else {
					// record name is NOT in the references, and the name has been seen
					if ref, ok := seenRecords[recordName]; ok {
						// record is not the same so we are duplicated
						if ref != subRecord {
							return subRecord.Name(), true
						}
					} else {
						// record name is NOT in the references, and the name has not been seen
						seenRecords[recordName] = subRecord
						return isAvroOverrideReferenceName(references, subRecord, seenRecords)
					}
				}
			}
		}

		// field is record
		if subRecord, ok := field.Type().(*avro.RecordSchema); ok {
			recordName := subRecord.FullName()

			// record name is in the references
			if ref, ok := references[recordName]; ok {
				// record is not the reference so we are duplicated
				if ref != subRecord {
					// return normal name since it's overwriting in the same namespace
					return subRecord.Name(), true
				}

				// record is the reference so continue to new field
				continue
			} else {
				// record name is NOT in the references, and the name has been seen
				if ref, ok := seenRecords[recordName]; ok {
					// record is not the same so we are duplicated
					if ref != subRecord {
						return subRecord.Name(), true
					}
				} else {
					// record name is NOT in the references, and the name has not been seen
					seenRecords[recordName] = subRecord
					return isAvroOverrideReferenceName(references, subRecord, seenRecords)
				}
			}
		}
	}

	return "", false
}
