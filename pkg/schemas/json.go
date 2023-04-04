package schemas

import (
	"fmt"
	"math/big"

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

// Following rules here https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.html#json-schema-compatibility-rules
// Plus some obvious things that are not strictly covered but would break compatibility
// TODO: is there a golang library that can do this for me similar to avro?
//
//	this is getting too crazy to actually support and make sense of
//	I am not an expert on json schemas and there is a lot of nuance
//	so I'm not really sure I want to actually support this right now
func (s *ParsedJSONSchema) isBackwardsCompatible(reader, writer *jsonschema.Schema) (bool, error) {

	// TODO: Draft, not all drafts are backward compatible
	//  I have no idea the proper way to handle this

	// TODO: Format

	// TODO: Always

	// TODO: Ref

	// TODO: RecursiveAnchor

	// TODO: RecursiveRef

	// TODO: DynamicAnchor

	// TODO: DynamicRef

	// TODO: constant

	// TODO: enum

	// TODO: Not

	// TODO: AllOf - this may not have a type so we can't recurse
	// TODO: AnyOf
	// TODO: OneOf - unions https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.html#union-compatibility

	// TODO: If
	// TODO: Then
	// TODO: Else

	readerType := reader.Types[0]
	switch writerType := writer.Types[0]; writerType {
	case "string":
		// types must match
		if writerType != readerType {
			return false, nil
		}

		// if writer min length is less than reader, not compatible
		if writer.MinLength < reader.MinLength {
			return false, nil
		}

		// if writer max length is greater than reader, not compatible
		if writer.MaxLength > reader.MaxLength {
			return false, nil
		}

		// if writer and reader have a pattern, and they are not the same pattern, not compatible
		if (writer.Pattern != nil && reader.Pattern != nil) && (writer.Pattern.String() != reader.Pattern.String()) {
			return false, nil
		}

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

		// if the reader has a minimum
		if reader.Minimum != nil {
			// and the writer does not, not compatible
			if writer.Minimum == nil {
				return false, nil
			}

			// and the writer is less than reader, not compatible
			if writer.Minimum.Cmp(reader.Minimum) == -1 {
				return false, nil
			}
		}

		// if the reader has a maximum {
		if reader.Maximum != nil {
			// and the writer does not, not compatible
			if writer.Maximum == nil {
				return false, nil
			}

			// and the writer is greater than the reader, not compatible
			if writer.Maximum.Cmp(reader.Maximum) == 1 {
				return false, nil
			}
		}

		// if the reader has an exclusive minimum
		if reader.ExclusiveMinimum != nil {
			// and the writer does not, not compatible
			if writer.ExclusiveMinimum == nil {
				return false, nil
			}

			// and the writer is less than reader, not compatible
			if writer.ExclusiveMinimum.Cmp(reader.ExclusiveMinimum) == -1 {
				return false, nil
			}
		}

		// if the reader has an exclusive maximum {
		if reader.ExclusiveMaximum != nil {
			// and the writer does not, not compatible
			if writer.ExclusiveMaximum == nil {
				return false, nil
			}

			// and the writer is greater than the reader, not compatible
			if writer.ExclusiveMaximum.Cmp(reader.ExclusiveMaximum) == 1 {
				return false, nil
			}
		}

		// if the reader has a multipleOf
		if reader.MultipleOf != nil {
			// and the writer does not, not compatible
			if writer.MultipleOf == nil {
				return false, nil
			}

			// and the writer is not a multiple of the reader, not compatible
			if q := new(big.Rat).Quo(writer.MultipleOf, reader.MultipleOf); !q.IsInt() {
				return false, nil
			}
		}
	case "object":
		// types must match
		if writerType != readerType {
			return false, nil
		}

		// if writer min properties is less than reader, not compatible
		if writer.MinProperties < reader.MinProperties {
			return false, nil
		}

		// if writer max properties is greater than reader, not compatible
		if writer.MaxProperties > reader.MaxProperties {
			return false, nil
		}

		// make sure reader required is in the writer
		for _, readerRequired := range reader.Required {
			found := false

			for _, writerRequired := range writer.Required {
				if readerRequired == writerRequired {
					found = true
					break
				}
			}

			// reader required is not in the writer, not compatible
			if !found {
				return false, nil
			}
		}

		// TODO: properties

		// TODO: property names

		// TODO: RegexProperties

		// TODO: PatternProperties

		// TODO: AdditionalProperties

		// TODO: Dependencies

		// TODO: DependentRequired

		// TODO: DependentSchemas

		// TODO: UnevaluatedProperties

		break
	case "array":
		// types must match
		if writerType != readerType {
			return false, nil
		}

		// if writer min items is less than reader, not compatible
		if writer.MinItems < reader.MinItems {
			return false, nil
		}

		// if writer max items is greater than reader, not compatible
		if writer.MaxItems > reader.MaxItems {
			return false, nil
		}

		// if writer unique is not unique but reader is, not compatible
		if !writer.UniqueItems && reader.UniqueItems {
			return false, nil
		}

		// if reader has items
		if reader.Items != nil {
			// and the writer does not, not compatible
			if writer.Items == nil {
				return false, nil
			}

			switch v := writer.Items.(type) {
			case *jsonschema.Schema:
				// if reader is not also schema, not compatible
				if _, ok := reader.Items.(*jsonschema.Schema); !ok {
					return false, nil
				}

				// recurse
				return s.isBackwardsCompatible(reader.Items.(*jsonschema.Schema), v)
			case []*jsonschema.Schema:
				// if reader is not also schema list, not compatible
				if _, ok := reader.Items.([]*jsonschema.Schema); !ok {
					return false, nil
				}

				readerItems := reader.Items.([]*jsonschema.Schema)

				// if items length not the same, not compatible
				if len(v) != len(readerItems) {
					// TODO: can the writer have more items if something like additionalItems is set?
					return false, nil
				}

				// check each item if it is compatible
				for itemIndex, item := range v {
					compatible, err := s.isBackwardsCompatible(readerItems[itemIndex], item)
					if err != nil {
						return false, err
					}

					if compatible == false {
						return false, err
					}
				}

				break
			}
		}

		// TODO: additionalItems

		// TODO: prefixItems

		// if reader has Items2020
		if reader.Items2020 != nil {
			// and the writer does not, not compatible
			if writer.Items2020 == nil {
				return false, nil
			}

			// recurse
			return s.isBackwardsCompatible(reader.Items2020, writer.Items2020)
		}

		// if reader has contains
		if reader.Contains != nil {
			// and the writer does not, not compatible
			if writer.Contains == nil {
				return false, nil
			}

			// recurse
			return s.isBackwardsCompatible(reader.Contains, writer.Contains)
		}

		// TODO: containsEval?

		// if writer min contains is less than reader, not compatible
		if writer.MinContains < reader.MinContains {
			return false, nil
		}

		// if writer max contains is greater than reader, not compatible
		if writer.MaxContains > reader.MaxContains {
			return false, nil
		}

		// TODO: UnevaluatedItems

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
