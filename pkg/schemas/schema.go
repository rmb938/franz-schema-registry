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
				return nil, fmt.Errorf("error parsing avro schema reference: %w", err)
			}
		}

		avroSchema, err := avro.ParseWithCache(rawSchema, "", avroCache)
		if err != nil {
			return nil, fmt.Errorf("error parsing avro schema: %w", err)
		}

		parsedSchema = &ParsedAvroSchema{
			avroSchema: avroSchema,
		}

	default:
		return nil, fmt.Errorf("unknown schema type: %s", schemaType)
	}

	return parsedSchema, nil
}
