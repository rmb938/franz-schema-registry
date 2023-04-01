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

	return s.isBackwardsCompatible(s.avroSchema, previousAvroSchema.avroSchema)
}

func (s *ParsedAvroSchema) isBackwardsCompatible(writerAvroSchema, readerAvroSchema avro.Schema) (bool, error) {
	// https://avro.apache.org/docs/1.11.1/specification/_print/#schema-resolution

	// reader is a union but writer is not
	// the first schema in the reader that matches is recursively resolve
	// if none match we are not backwards compatible
	if writerAvroSchema.Type() != avro.Union && readerAvroSchema.Type() == avro.Union {
		for _, readersType := range readerAvroSchema.(*avro.UnionSchema).Types() {
			if readersType.Type() == writerAvroSchema.Type() {
				return s.isBackwardsCompatible(writerAvroSchema, readersType)
			}
		}
		return false, nil
	}

	switch v := writerAvroSchema.(type) {
	case *avro.PrimitiveSchema:
		readerPrimitive, ok := readerAvroSchema.(*avro.PrimitiveSchema)

		// if the reader is also a primitive
		if ok {
			readerType := readerPrimitive.Type()
			switch v.Type() {
			case avro.Int:
				if readerType == avro.Int || readerType == avro.Long || readerType == avro.Float || readerType == avro.Double {
					return true, nil
				}
				break
			case avro.Long:
				if readerType == avro.Long || readerType == avro.Float || readerType == avro.Double {
					return true, nil
				}
				break
			case avro.Float:
				if readerType == avro.Float {
					return true, nil
				}
				break
			case avro.String:
				if readerType == avro.String || readerType == avro.Bytes {
					return true, nil
				}
				break
			case avro.Bytes:
				if readerType == avro.Bytes || readerType == avro.String {
					return true, nil
				}
				break
			default:
				if v.Type() == readerPrimitive.Type() {
					return true, nil
				}
				break
			}
		}

		return false, nil
	case *avro.RecordSchema:
		if readerAvroSchema.Type() == avro.Record {
			recordReaderAvroSchema := readerAvroSchema.(*avro.RecordSchema)

			if v.Name() != recordReaderAvroSchema.Name() {
				return false, nil
			}

			// check if reader fields are in writer, if they are recursively check the schema
			// if they are not and if they don't have a default we are not backward compatible
			for _, readerField := range recordReaderAvroSchema.Fields() {
				found := false
				for _, writerField := range v.Fields() {
					if readerField.Name() == writerField.Name() {
						found = true
						isBackwardsCompatible, err := s.isBackwardsCompatible(writerField.Type(), readerField.Type())
						if err != nil {
							return false, err
						}
						if isBackwardsCompatible == false {
							return false, err
						}
					}
				}
				// field is not in the writer
				if !found {
					// and the reader field has no default we are not backward compatible
					if readerField.HasDefault() == false {
						return false, nil
					}
				}
			}

			return true, nil
		}
		break
	case *avro.EnumSchema:
		// reader is an enum as well, so we need to check if all writer enum symbols are in the reader
		// if they are not and the reader does not have a default set then we are not backward compatible
		if readerAvroSchema.Type() == avro.Enum {
			enumReaderAvroSchema := readerAvroSchema.(*avro.EnumSchema)
			for _, writerSymbol := range v.Symbols() {
				found := false
				for _, readerSymbol := range enumReaderAvroSchema.Symbols() {
					if writerSymbol == readerSymbol {
						found = true
						break
					}
				}
				// writer enum is not in the reader
				if !found {
					// reader does not have a default, so it's not backward compatible
					if len(enumReaderAvroSchema.Default()) == 0 {
						return false, nil
					}
				}
			}
		}
		return true, nil
	case *avro.ArraySchema:
		// reader is an array as well, so we need to check backwards compatibility recursively
		if readerAvroSchema.Type() == avro.Array {
			return s.isBackwardsCompatible(v.Items(), readerAvroSchema.(*avro.ArraySchema).Items())
		}
		break
	case *avro.MapSchema:
		// reader is a map as well, so we need to check backwards compatibility recursively
		if readerAvroSchema.Type() == avro.Map {
			return s.isBackwardsCompatible(v.Values(), readerAvroSchema.(*avro.MapSchema).Values())
		}
		break
	case *avro.UnionSchema:
		// reader is a union as well, so we need at least one of them to match
		// if none are matched we are not compatible
		if readerAvroSchema.Type() == avro.Union {
			for _, readersType := range readerAvroSchema.(*avro.UnionSchema).Types() {
				for _, writersTypes := range v.Types() {
					isBackwardsCompatible, err := s.isBackwardsCompatible(writersTypes, readersType)
					if err != nil {
						return false, err
					}
					if isBackwardsCompatible {
						return true, nil
					}
				}
			}
			return false, nil
		}
		break
	case *avro.FixedSchema:
		if readerAvroSchema.Type() == avro.Fixed {
			fixedReaderAvroSchema := readerAvroSchema.(*avro.FixedSchema)

			if v.Name() != fixedReaderAvroSchema.Name() {
				return false, nil
			}

			if v.Size() != fixedReaderAvroSchema.Size() {
				return false, nil
			}

			return true, nil
		}
		break
	case *avro.RefSchema:
		if readerAvroSchema.Type() == avro.Ref {
			refReaderAvroSchema := readerAvroSchema.(*avro.RefSchema)

			if v.String() != refReaderAvroSchema.String() {
				return false, nil
			}

			return true, nil
		}
		break
	default:
		return false, fmt.Errorf("missing compatibility check for avro type: %T", v)
	}

	return false, nil
}
