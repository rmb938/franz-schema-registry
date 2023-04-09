package schemas

import (
	"fmt"
	"math/big"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"golang.org/x/exp/slices"
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
	//  we don't want to return the recurse just keep adding to set of differences

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

	// normalize schema, if it points to a ref, get the ref instead
	if reader.Ref != nil {
		reader = reader.Ref
	}
	if writer.Ref != nil {
		writer = writer.Ref
	}

	// TODO: combined checking https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/SchemaDiff.java#L132-L165

	// if len(Types) == 0 schema is `{}` or true/false
	writerType := ""
	readerType := ""
	if len(writer.Types) != 0 {
		writerType = writer.Types[0]
	}
	if len(reader.Types) != 0 {
		readerType = reader.Types[0]
	}

	writerCompareType := writerType
	readerCompareType := readerType
	// normalize integer to number so comparison passes
	if writerCompareType == "integer" {
		writerCompareType = "number"
	}
	if readerCompareType == "integer" {
		readerCompareType = "number"
	}

	// https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/SchemaDiff.java#L167-L175
	// type is different
	if writerCompareType != readerCompareType {
		// reader is false schema, compatible
		if reader.Always != nil && *reader.Always == false {
			return true, nil
		}

		// writer is true schema or empty, compatible
		if (writer.Always != nil && *writer.Always == false) || len(writerCompareType) == 0 {
			return true, nil
		}

		// type changed, not compatible
		return false, nil
	}

	// https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/EnumSchemaDiff.java#L25
	writerContainsAllReaderEnums := true
	readerContainsAllWriterEnums := true
	for _, item := range writer.Enum {
		contains := slices.Contains(reader.Enum, item)
		if !contains {
			writerContainsAllReaderEnums = false
		}
	}
	for _, item := range reader.Enum {
		contains := slices.Contains(writer.Enum, item)
		if !contains {
			readerContainsAllWriterEnums = false
		}
	}
	if writerContainsAllReaderEnums {
		// enum array extended, compatible
	} else if readerContainsAllWriterEnums {
		// enum array narrowed, not compatible
		return false, nil
	} else {
		// enum array changed, not compatible
		return false, nil
	}

	// https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/NotSchemaDiff.java#L24
	notIsBackwardsCompatible, err := s.isBackwardsCompatible(reader.Not, reader.Not)
	if err != nil {
		return false, err
	}
	if notIsBackwardsCompatible {
		// not type compatible; not type narrowed, compatible
	} else {
		// not type not compatible; not type extended, not compatible
		return false, nil
	}

	// TODO: allOf, anyOf, oneOf; https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/CombinedSchemaDiff.java#L38

	switch writerType {
	case "string":
		if reader.MaxLength == -1 && writer.MaxLength != -1 {
			// max length added, not compatible
			return false, nil
		} else if reader.MaxLength != -1 && writer.MaxLength == -1 {
			// max length removed, compatible
		} else if reader.MaxLength < writer.MaxLength {
			// max length increased, compatible
		} else if reader.MaxLength > writer.MaxLength {
			// max length decreases, not compatible
			return false, nil
		}

		if reader.MinLength == -1 && writer.MinLength != -1 {
			// min length added, not compatible
			return false, nil
		} else if reader.MinLength != -1 && writer.MinLength == -1 {
			// min length remove, compatible
		} else if reader.MinLength < writer.MinLength {
			// min length increased, not compatible
			return false, nil
		} else if reader.MinLength > writer.MinLength {
			// min length decreased, compatible
		}

		if reader.Pattern == nil && writer.Pattern != nil {
			// pattern added, not compatible
			return false, nil
		} else if reader.Pattern != nil && writer.Pattern == nil {
			// pattern remove, compatible
		} else if reader.Pattern.String() != writer.Pattern.String() {
			// pattern changed, not compatible
			return false, nil
		}

		break
	case "integer", "number":
		if writerType != readerType {
			if writerType == "integer" {
				// writer is integer while reader isn't; type narrowed, not compatible
				return false, nil
			} else {
				// writer is number and reader is int; type extended, compatbie
			}
		}

		if reader.Maximum == nil && writer.Maximum != nil {
			// maximum added, not compatible
			return false, nil
		} else if reader.Maximum != nil && writer.Maximum == nil {
			// maximum removed, compatible
		} else if reader.Maximum.Cmp(writer.Maximum) == -1 {
			// maximum increased, compatible
		} else if reader.Maximum.Cmp(writer.Maximum) == 1 {
			// maximum decreased, not compatible
			return false, nil
		}

		if reader.Minimum == nil && writer.Minimum != nil {
			// minimum added, not compatible
			return false, nil
		} else if reader.Minimum != nil && writer.Minimum == nil {
			// minimum removed, compatible
		} else if reader.Minimum.Cmp(writer.Minimum) == -1 {
			// minimum increased, not compatible
			return false, nil
		} else if reader.Minimum.Cmp(writer.Minimum) == 1 {
			// minimum decreased, compatible
		}

		if reader.ExclusiveMaximum == nil && writer.ExclusiveMaximum != nil {
			// exclusive maximum added, not compatible
			return false, nil
		} else if reader.ExclusiveMaximum != nil && writer.ExclusiveMaximum == nil {
			// exclusive maximum removed, compatible
		} else if reader.ExclusiveMaximum.Cmp(writer.ExclusiveMaximum) == -1 {
			// exclusive maximum increased, compatible
		} else if reader.ExclusiveMaximum.Cmp(writer.ExclusiveMaximum) == 1 {
			// exclusive maximum decreased, not compatible
			return false, nil
		}

		if reader.ExclusiveMinimum == nil && writer.ExclusiveMinimum != nil {
			// exclusive minimum added, not compatible
			return false, nil
		} else if reader.ExclusiveMinimum != nil && writer.ExclusiveMinimum == nil {
			// exclusive minimum removed, compatible
		} else if reader.ExclusiveMinimum.Cmp(writer.ExclusiveMinimum) == -1 {
			// exclusive minimum increased, not compatible
			return false, nil
		} else if reader.ExclusiveMinimum.Cmp(writer.ExclusiveMinimum) == 1 {
			// exclusive minimum decreased, compatible
		}

		if reader.MultipleOf == nil && writer.MultipleOf != nil {
			// multiple added, not compatible
			return false, nil
		} else if reader.MultipleOf != nil && writer.MultipleOf == nil {
			// multiple removed, compatible
		} else if new(big.Int).Mod(writer.MultipleOf.Num(), reader.MultipleOf.Num()).Cmp(big.NewInt(0)) == 0 {
			// multiple expanded, not compatible
			return false, nil
		} else if new(big.Int).Mod(reader.MultipleOf.Num(), writer.MultipleOf.Num()).Cmp(big.NewInt(0)) == 0 {
			// multiple reduced, compatible
		} else {
			// multiple changed, not compatible
			return false, nil
		}
		break
	case "object":
		if reader.MaxProperties == -1 && writer.MaxProperties != -1 {
			// max properties added, not compatible
			return false, nil
		} else if reader.MaxProperties != -1 && writer.MaxProperties == -1 {
			// max properties removed, compatible
		} else if reader.MaxProperties < writer.MaxProperties {
			// max properties increased, compatible
		} else if reader.MaxProperties > writer.MaxProperties {
			// max properties decreased, not compatible
			return false, nil
		}

		if reader.MinProperties == -1 && writer.MinProperties != -1 {
			// min properties added, not compatible
			return false, nil
		} else if reader.MinProperties != -1 && writer.MinProperties == -1 {
			// min properties removed, compatible
		} else if reader.MinProperties < writer.MinProperties {
			// min properties increased, not compatible
			return false, nil
		} else if reader.MinProperties > writer.MinProperties {
			// min properties decreased, compatible
		}

		readerPermitsAdditionalProps := false
		writerPermitsAdditionalProps := false
		if reader.AdditionalProperties != nil {
			if b, ok := reader.AdditionalProperties.(bool); ok {
				readerPermitsAdditionalProps = b
			}
		}
		if writer.AdditionalProperties != nil {
			if b, ok := writer.AdditionalProperties.(bool); ok {
				writerPermitsAdditionalProps = b
			}
		}
		readerAdditionalPropsSchema, _ := reader.AdditionalProperties.(*jsonschema.Schema)
		writerAdditionalPropsSchema, _ := writer.AdditionalProperties.(*jsonschema.Schema)
		if readerPermitsAdditionalProps != writerPermitsAdditionalProps {
			if writerPermitsAdditionalProps {
				// additional properties added, compatible
			} else {
				// additional properties removed, not compatible
				return false, nil
			}
		} else if readerAdditionalPropsSchema == nil && writerAdditionalPropsSchema != nil {
			// additional properties narrowed, not compatible
			return false, nil
		} else if readerAdditionalPropsSchema != nil && writerAdditionalPropsSchema == nil {
			// additional properties extended, compatible
		} else {
			additionalPropsBackwardCompatible, err := s.isBackwardsCompatible(readerAdditionalPropsSchema, writerAdditionalPropsSchema)
			if err != nil {
				return false, err
			}
			if !additionalPropsBackwardCompatible {
				// additional props not compatible, not compatible
				return false, nil
			}
		}

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

		// combine keys from reader.Properties & writer.Properties
		// loop keys
		//  readerSchema := reader.Properties[key]
		//  writerSchema := writer.Properties[key]
		//  if writerSchema == nil
		//      if writer is open content model
		//          property removed from open content model, compatible
		//      else
		//          writerPartialSchema := writer.PatternProps[key] <- need to loop and see if regex matches
		//          if writerPartialSchema != nil
		//              isCompatible, err := isBackwardsCompatible(readerSchema, writerPartialSchema)
		//              if isCompatible
		//                  property removed is covered by partially open content model, compatible
		//              else
		//                  property removed not covered by partially open content model, not compatible
		//          else
		//              if readerSchema is false schema TODO: how do we do this?
		//                  property with false removed from closed content model, compatible
		//              else
		//                  property removed from closed content model, not compatible
		//  else if readerSchema == nil
		//      if reader is open content model
		//          if writer is empty schema TODO: how do we do this? types len == 0?
		//              property with empty schema added to open content model, compatible
		//          else
		//              property added to open content model, not compatible
		//      else
		//          readerPartialSchema := reader.PatternProps[key] <- need to loop and see if regex matches
		//          if readerPartialSchema != nil
		//              isCompatible, err := isBackwardsCompatible(readerPartialSchema, writerSchema)
		//              if isCompatible
		//                  property added is covered by partially open content model, compatible
		//              else
		//                  property added is not covered by partially open content model, not compatible
		//          else
		//      if writer.Required contains key
		//          if writer.Property[key].hasDefault
		//              required property with default added to unopen content model, compatible
		//          else
		//              required property added to unopen content model, not compatible
		//      else
		//          optional property added to unopen content model, compatible
		//  else
		//    recurse & compare schema

		for readerPropKey, _ := range reader.Properties {
			if _, ok := writer.Properties[readerPropKey]; ok {
				readerRequired := slices.Contains(reader.Required, readerPropKey)
				writerRequired := slices.Contains(writer.Required, readerPropKey)
				if readerRequired && !writerRequired {
					// required attribute removed, compatible
				} else if !readerRequired && writerRequired {
					if writer.Properties[readerPropKey].Default != nil {
						// required attribute with default added, compatible
					} else {
						// required attribute added, not compatible
						return false, nil
					}
				}
			}
		}
		break
	case "array":
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

		// if items is schema
		//  recurse & compare schemas

		// if items is array of schema
		//  TODO: complicated things; see https://github.com/confluentinc/schema-registry/blob/9ef76b4a1373f50a505162e72cffcbfd3dd2fee3/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff/ArraySchemaDiff.java#L121

		break
	case "boolean":
		break
	case "null":
		break
	case "": // empty schema (or true/false)
		// TODO: empty is always compatible with empty but is true always compatible with false and vise versa?
		//  it doesn't look like confluent sr compares these so I think so?
		break
	default:
		return false, fmt.Errorf("unknown json schema type: %s", writerType)
	}

	return true, nil
}
