package subjects

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/rmb938/franz-schema-registry/pkg/schemas"
)

type SubjectReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int32  `json:"version"`
}

type RequestPostSubjectVersion struct {
	Schema     string             `json:"schema"`
	SchemaType schemas.SchemaType `json:"schemaType"`
	References []SubjectReference `json:"references,omitempty"`

	calculatedHash string
}

func (r *RequestPostSubjectVersion) Bind(request *http.Request) error {
	if len(r.Schema) == 0 {
		return fmt.Errorf("schema may not be empty")
	}

	hash128 := fnv.New128a()
	if _, err := hash128.Write([]byte(r.Schema)); err != nil {
		return fmt.Errorf("error calculating hash of schema: %w", err)
	}

	foundReferenceNames := map[string]interface{}{}
	for _, reference := range r.References {
		if _, ok := foundReferenceNames[reference.Name]; ok {
			return fmt.Errorf("duplicate reference name %s", reference.Name)
		}
		foundReferenceNames[reference.Name] = nil
	}

	// add references to the hash, references also make the schema unique
	// sort references by name before we add them so if only the ordering changes the hash is the same
	sort.Slice(r.References, func(i, j int) bool {
		cmp := strings.Compare(r.References[i].Name, r.References[j].Name)

		if cmp < 0 {
			return true
		}

		return false
	})
	for _, reference := range r.References {
		if _, err := hash128.Write([]byte(reference.Name)); err != nil {
			return fmt.Errorf("error calculating hash of schema: %w", err)
		}
		if _, err := hash128.Write([]byte(reference.Subject)); err != nil {
			return fmt.Errorf("error calculating hash of schema: %w", err)
		}
		if _, err := hash128.Write([]byte(strconv.Itoa(int(reference.Version)))); err != nil {
			return fmt.Errorf("error calculating hash of schema: %w", err)
		}
	}

	r.calculatedHash = hex.EncodeToString(hash128.Sum(nil))

	return nil
}

type ResponsePostSubjectVersion struct {
	ID int32 `json:"id"`
}

func (r *ResponsePostSubjectVersion) Render(writer http.ResponseWriter, request *http.Request) error {
	return nil
}

type RequestPostSubject struct {
	Schema     string             `json:"schema"`
	SchemaType schemas.SchemaType `json:"schemaType"`
	References SubjectReference   `json:"references,omitempty"`

	calculatedHash string
}

func (r *RequestPostSubject) Bind(request *http.Request) error {
	if len(r.Schema) == 0 {
		return fmt.Errorf("schema may not be empty")
	}

	hash128 := fnv.New128a()
	if _, err := hash128.Write([]byte(r.Schema)); err != nil {
		return fmt.Errorf("error calculating hash of schema: %w", err)
	}

	r.calculatedHash = hex.EncodeToString(hash128.Sum(nil))

	return nil
}

type ResponsePostSubject struct {
	Subject string `json:"subject"`
	ID      int32  `json:"id"`
	Version int32  `json:"version"`
	Schema  string `json:"schema"`
}

func (r *ResponsePostSubject) Render(writer http.ResponseWriter, request *http.Request) error {
	return nil
}

type ResponseGetSubjectVersion struct {
	Subject    string             `json:"subject"`
	ID         int32              `json:"id"`
	Version    int32              `json:"version"`
	SchemaType schemas.SchemaType `json:"schemaType,omitempty"`
	Schema     string             `json:"schema"`
}

func (r *ResponseGetSubjectVersion) Render(writer http.ResponseWriter, request *http.Request) error {
	return nil
}
