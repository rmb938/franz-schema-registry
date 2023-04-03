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

	// TODO: this compatible check has a bug https://github.com/hamba/avro/issues/249
	//  that doesn't take into account field/symbol/union order changes which would make the schema not compatible
	compatibilityErr := schemaCompat.Compatible(previousAvroSchema.avroSchema, s.avroSchema)
	if compatibilityErr != nil {
		return false, nil
	}

	return true, nil
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
