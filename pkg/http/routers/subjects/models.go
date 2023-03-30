package subjects

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
)

type SubjectReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeJSON     SchemaType = "JSON"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

type RequestPostSubjectVersion struct {
	Schema     string           `json:"schema"`
	SchemaType SchemaType       `json:"schemaType"`
	References SubjectReference `json:"references,omitempty"`

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
	Schema     string           `json:"schema"`
	SchemaType SchemaType       `json:"schemaType"`
	References SubjectReference `json:"references,omitempty"`

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
	Subject    string     `json:"subject"`
	ID         int32      `json:"id"`
	Version    int32      `json:"version"`
	SchemaType SchemaType `json:"schemaType,omitempty"`
	Schema     string     `json:"schema"`
}

func (r *ResponseGetSubjectVersion) Render(writer http.ResponseWriter, request *http.Request) error {
	return nil
}
