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

func ParseSchema(rawSchema string, schemaType SchemaType, references map[string]string) (ParsedSchema, error) {
	var parsedSchema ParsedSchema

	switch schemaType {
	case SchemaTypeAvro:
		avroCache := &avro.SchemaCache{}

		for _, reference := range references {
			_, err := avro.ParseWithCache(reference, "", avroCache)
			if err != nil {
				return nil, fmt.Errorf("error parsing avro schema reference %s: %w", reference, err)
			}
		}

		avroSchema, err := avro.ParseWithCache(rawSchema, "", avroCache)
		if err != nil {
			return nil, fmt.Errorf("error parsing avro schema: %w", err)
		}

		// make sure we aren't doing weird naming things
		if namedSchema, ok := avroSchema.(avro.NamedSchema); ok {
			// make sure our schema's name isn't the same as a reference
			if name, ok := isAvroOverrideReferenceName(references, namedSchema); ok {
				return nil, fmt.Errorf("can't redefine: %s", name)
			}

			// check if self referencing
			//  while self-references are allowed per avro spec it is not allowed in schema registry
			if recordSchema, ok := avroSchema.(*avro.RecordSchema); ok {
				if isAvroSelfReferencing(recordSchema.Name(), recordSchema) {
					fmt.Println(avroSchema.String())
					return nil, fmt.Errorf("can't self-reference: %s", namedSchema.Name())
				}
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

func isAvroOverrideReferenceName(references map[string]string, namedSchema avro.NamedSchema) (string, bool) {
	if _, ok := references[namedSchema.Name()]; ok {
		return namedSchema.Name(), true
	}

	if recordSchema, ok := namedSchema.(*avro.RecordSchema); ok {
		for _, field := range recordSchema.Fields() {
			if refSchema, ok := field.Type().(*avro.RefSchema); ok {
				if subNamedSchema, ok := refSchema.Schema().(avro.NamedSchema); ok {
					return isAvroOverrideReferenceName(references, subNamedSchema)
				}
			}
			if subRecord, ok := field.Type().(*avro.RecordSchema); ok {
				return isAvroOverrideReferenceName(references, subRecord)
			}
		}
	}

	return "", false
}

func isAvroSelfReferencing(name string, recordSchema *avro.RecordSchema) bool {
	for _, field := range recordSchema.Fields() {
		if refSchema, ok := field.Type().(*avro.RefSchema); ok {
			if namedSchema, ok := refSchema.Schema().(avro.NamedSchema); ok {
				if namedSchema.Name() == name {
					fmt.Printf("Field %s is a reference to %s\n", field.Name(), namedSchema.Name())
					return true
				}

				if subRecord, ok := namedSchema.(*avro.RecordSchema); ok {
					return isAvroSelfReferencing(name, subRecord)
				}
			}
		}

		if subRecord, ok := field.Type().(*avro.RecordSchema); ok {
			return isAvroSelfReferencing(name, subRecord)
		}
	}

	return false
}
