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

func ParseSchema(rawSchema string, schemaType SchemaType) (ParsedSchema, error) {
	var parsedSchema ParsedSchema

	switch schemaType {
	case SchemaTypeAvro:
		avroSchema, err := avro.Parse(rawSchema)
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
