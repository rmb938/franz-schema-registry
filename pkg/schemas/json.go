package schemas

import (
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type ParsedJSONSchema struct {
	jsonSchema *jsonschema.Schema
}

func (s *ParsedJSONSchema) IsBackwardsCompatible(previousSchema ParsedSchema) (bool, error) {
	_, ok := previousSchema.(*ParsedJSONSchema)
	if !ok {
		return false, fmt.Errorf("cannot check compatibility, previous schema isn't json")
	}

	// TODO: this

	return true, nil
}
