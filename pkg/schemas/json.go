package schemas

import (
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type ParsedJSONSchema struct {
	jsonSchema *jsonschema.Schema
}

func (s *ParsedJSONSchema) IsBackwardsCompatible(previousSchema ParsedSchema) (bool, error) {
	previousJsonSchema, ok := previousSchema.(*ParsedJSONSchema)
	if !ok {
		return false, fmt.Errorf("cannot check compatibility, previous schema isn't json")
	}

	reader := previousJsonSchema.jsonSchema
	writer := s.jsonSchema

	return s.isBackwardsCompatible(reader, writer)
}

// Following rules here https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/SchemaDiff.java#L118
func (s *ParsedJSONSchema) isBackwardsCompatible(reader, writer *jsonschema.Schema) (bool, error) {
	// TODO: eventually we probably want to do something similar as confluent SR where it will return with the issues instead of just true/false

	// both schemas are nil so they are compatible
	if reader == nil && writer == nil {
		return true, nil
	} else if reader == nil {
		// reader is nil so we are compatible
		return true, nil
	} else if writer == nil {
		// writer is nil so we are not compatible
		return false, nil
	}

	// enum array items added, compatible

	// enum array items removed, not compatible

	// enum changed, not compatible; what does this mean?

	// not type compatible, compatible
	// not type not compatible, not compatible
	notIsBackwardsCompatible, err := s.isBackwardsCompatible(reader.Not, reader.Not)
	if err != nil {
		return false, err
	}
	if !notIsBackwardsCompatible {
		return false, nil
	}

	// TODO: allOf, anyOf, oneOf; https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/CombinedSchemaDiff.java#L38

	writerType := writer.Types[0]
	readerType := reader.Types[0]

	switch writerType {
	case "string":
		// types must match
		if readerType != readerType {
			return false, nil
		}

		// max length added, not compatible

		// max length removed, compatible

		// max length increased, compatible

		// max length decreases, not compatible

		// min length added, not compatible

		// min length remove, compatible

		// min length increased, not compatible

		// min length decreased, compatible

		// pattern added, not compatible

		// pattern remove, compatible

		// pattern changed, not compatible

		break
	case "integer", "number":
		// if writer is a number and reader is not, not compatible
		if writerType == "number" && readerType != "number" {
			return false, nil
		}

		// if writer is an integer and reader is not an integer or a number, not compatible
		if writerType == "integer" && (readerType != "integer" && readerType != "number") {
			return false, nil
		}

		// maximum added, not compatible

		// maximum removed, compatible

		// maximum increased, compatible

		// maximum decreased, not compatible

		// minimum added, not compatible

		// minimum removed, compatible

		// minimum increased, not compatible

		// minimum decreased, compatible

		// exclusive maximum added, not compatible

		// exclusive maximum remove, compatible

		// exclusive maximum increased, compatible

		// exclusive maximum decreased, not compatible

		// exclusive minimum added, not compatible

		// exclusive minimum remove, compatible

		// exclusive minimum increased, not compatible

		// exclusive minimum decreased, compatible

		// multiple added, not compatible

		// multiple removed, compatible

		// multiple expanded, not compatible

		// multiple reduced, compatible

		// multiple changed, not compatible; what's this?
	case "object":
		// types must match
		if writerType != readerType {
			return false, nil
		}

		break
	case "array":
		// types must match
		if writerType != readerType {
			return false, nil
		}

		// max items added, not compatible

		// max items removed, compatible

		// max items increased, compatible

		// max items decreased, not compatible

		// min items added, not compatible

		// min items removed, compatible

		// min items increased, not compatible

		// min items decreased, compatible

		// unique items removed, compatible

		// unique items added, not compatible

		// additional items is either nil, bool or *Schema
		// if additional items is bool for both and not equal
		//  if reader is true
		//      additional items removed, not compatible
		//  else
		//      additional items added, compatible
		// else if additional items either nil or schema
		//  if reader == nil && writer != nil
		//      additional items narrowed, not compatible
		//  else if reader != nil && writer == nil
		//      additional items extended, compatible
		//  else
		//      recurse & compare additional items schema

		// if items is array of schema
		//  complicated things; see https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/ArraySchemaDiff.java#L121
		// else items is schema
		//  recurse & compare schemas

		break
	case "boolean":
		// types must match
		if writerType != readerType {
			return false, nil
		}
		break
	case "null":
		// types must match
		if writerType != readerType {
			return false, nil
		}
		break
	default:
		return false, fmt.Errorf("unknown json schema type: %s", writerType)
	}

	return true, nil
}
