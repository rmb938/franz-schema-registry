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

	// This compatible check checks compatibility of the schema itself but does not guarantee compatibility of the encoded data
	// i.e. if record fields, enums, unions change order the encoded data is no longer compatible
	// Confluent SR: https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/client/src/main/java/io/confluent/kafka/schemaregistry/avro/AvroSchema.java#LL327C40-L327C40
	// Java Avro Library: https://github.com/apache/avro/blob/916a09ce852769b9172882957e4b766b2970dd52/lang/java/avro/src/main/java/org/apache/avro/SchemaCompatibility.java#LL262C50-L262C50
	// it looks like the Java Avro Library also doesn't guarantee compatibility of the encoded data
	// so this is probably ok
	compatibilityErr := schemaCompat.Compatible(previousAvroSchema.avroSchema, s.avroSchema)
	if compatibilityErr != nil {
		return false, nil
	}

	// TODO: eventually like json return the reasoning why, not just true/false

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
