package schemas

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

type ParsedAvroSchema struct {
	avroSchema avro.Schema
}

func (s *ParsedAvroSchema) IsBackwardsCompatible(previousSchema ParsedSchema) (bool, error) {
	previousAvroSchema, ok := previousSchema.(*ParsedAvroSchema)
	if !ok {
		return false, fmt.Errorf("cannot check compatibility, previous schema isn't avro")
	}

	schemaCompat := avro.NewSchemaCompatibility()
	compatibilityErr := schemaCompat.Compatible(previousAvroSchema.avroSchema, s.avroSchema)
	if compatibilityErr != nil {
		return false, nil
	}

	return true, nil
}
