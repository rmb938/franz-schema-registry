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
		// reader is nil; schema added, not compatible
		return false, nil
	} else if writer == nil {
		// writer is nil; schema removed, compatible
		return true, nil
	}

	// enum array extended, compatible

	// enum array narrowed, not compatible

	// enum array changed, not compatible

	// not type compatible; not type narrowed, compatible
	// not type not compatible; not type extended, not compatible
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
		// types must match; type changed, not compatible
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
		// if writer is a number and reader is not; type narrowed, not compatible
		if writerType == "number" && readerType != "number" {
			return false, nil
		}

		// if writer is an integer and reader is not an integer or a number; type changed, not compatible
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

		// multiple changed, not compatible
	case "object":
		// types must match; type changed, not compatible
		if writerType != readerType {
			return false, nil
		}

		// max properties added, not compatible

		// max properties removed, compatible

		// max properties increased, compatible

		// max properties decreased, not compatible

		// min properties added, not compatible

		// min properties removed, compatible

		// min properties increased, not compatible

		// min properties decreased, compatible

		// additional properties is either nil, bool or *Schema
		//  if reader is true
		//      additional properties added, compatible
		//  else
		//      additional properties removed, not compatible
		// else if additional properties either nil or schema
		//  if reader == nil && writer != nil
		//      additional properties narrowed, not compatible
		//  else if reader != nil && writer == nil
		//      additional properties extended, compatible
		//  else
		//      recurse & compare additional properties schema

		// combine keys from reader.Dependencies & writer.Dependencies
		// loop keys
		//   readerDependencies := get reader.Dependencies[key]
		//   writerDependencies := get writer.Dependencies[key]
		//   if writerDependencies == nil
		//      dependency removed, compatible
		//   else if readerDependencies == nil
		//      dependency added, not compatible
		//   else
		//      if both *Schema
		//          recurse & compare schema
		//      else if both []string
		//          if writer contains all reader
		//              dependency array extended, not compatible
		//          else if reader contains all writer
		//              dependency array narrowed, compatible
		//          else
		//              dependency array changed, not compatible

		// TODO: compare properties https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/ObjectSchemaDiff.java#L174

		// loop reader.Properties
		//      if writer.Properties contains key
		//          readerRequired := reader.Required contains key
		//          writerRequired := writer.Required contains key
		//          if readerRequired && not writerRequired
		//              required attribute removed, compatible
		//          else if not readerRequired && writerRequired
		//              if writer.Properties[key].hasDefault
		//                  required attribute with default added, compatible
		//              else
		//                  required attribute added, not compatible

		break
	case "array":
		// types must match; type changed, not compatible
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
		//  TODO: complicated things; see https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/ArraySchemaDiff.java#L121
		// else items is schema
		//  recurse & compare schemas

		break
	case "boolean":
		// types must match; type changed, not compatible
		if writerType != readerType {
			return false, nil
		}
		break
	case "null":
		// types must match; type changed, not compatible
		if writerType != readerType {
			return false, nil
		}
		break
	default:
		return false, fmt.Errorf("unknown json schema type: %s", writerType)
	}

	return true, nil
}
