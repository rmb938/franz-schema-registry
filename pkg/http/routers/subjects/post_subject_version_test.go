package subjects

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-chi/render"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"github.com/rmb938/franz-schema-registry/pkg/schemas"
	"github.com/stretchr/testify/assert"
)

func TestPostSubjectVersionBadAndInvalid(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// try to post on empty db
	resp, err := postSubjectVersion(db, nil, "unknown", &RequestPostSubjectVersion{})
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// try to post bad schema type
	resp, err = postSubjectVersion(db, nil, "unknown", &RequestPostSubjectVersion{
		SchemaType: schemas.SchemaType("bad"),
	})
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 400, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)

	// try to post on empty db good schema type
	resp, err = postSubjectVersion(db, nil, "unknown", &RequestPostSubjectVersion{
		SchemaType: schemas.SchemaTypeAvro,
	})
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// post subject invalid references
	resp, err = postSubjectVersion(db, nil, "one", &RequestPostSubjectVersion{
		Schema: `{"type": "string"}`,
		References: []SubjectReference{
			{
				Name:    "ref",
				Subject: "bad",
				Version: int32(1),
			},
		},
	})
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40402, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)
}

func TestPostSubjectVersionAvro(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// post good schema
	requestPostSubject := &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "long"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err := postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)

	// post the same schema again
	resp, err = postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)

	// post new version that is backward compatible
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "long"},
	{"name": "field2", "type": "string"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), resp.ID)

	// post a new version that is not backward compatible
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
	{"name": "field2", "type": "string"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "one", requestPostSubject)
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 409, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusConflict, w.Result().StatusCode)

	// delete subject
	_, err = deleteSubject(db, "one", false)
	assert.NoError(t, err)

	// recreate subject
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "long"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)
}

func TestPostSubjectVersionAvroReferences(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	requestPostSubject := &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "long"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err := postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)

	// create new schema that references one
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_two",
  "fields": [
    {"name": "field1", "type": "schema_one"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_one",
				Subject: "one",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "two", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), resp.ID)

	// create new version that changes reference so isn't compatible
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_two",
  "fields": [
    {"name": "field1", "type": "schema_two"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "two", requestPostSubject)
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 409, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusConflict, w.Result().StatusCode)
}

func TestPostSubjectVersionAvroReferencesLongChain(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	requestPostSubject := &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "long"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err := postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)

	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_two",
  "fields": [
    {"name": "field1", "type": "schema_one"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_one",
				Subject: "one",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "two", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), resp.ID)

	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_three",
  "fields": [
    {"name": "field1", "type": "schema_two"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_two",
				Subject: "two",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "three", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(3), resp.ID)

	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_four",
  "fields": [
    {"name": "field1", "type": "schema_three"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_three",
				Subject: "three",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "four", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(4), resp.ID)

	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_five",
  "fields": [
    {"name": "field1", "type": "schema_four"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_four",
				Subject: "four",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "five", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(5), resp.ID)

	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_six",
  "fields": [
    {"name": "field1", "type": "schema_five"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_five",
				Subject: "five",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "six", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(6), resp.ID)

	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_seven",
  "fields": [
    {"name": "field1", "type": "schema_six"}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_six",
				Subject: "six",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "seven", requestPostSubject)
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40902, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusConflict, w.Result().StatusCode)
}

func TestPostSubjectVersionAvroSelfReferences(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// create a new schema that references self
	requestPostSubject := &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_three",
  "fields": [
    {"name": "field1", "type": "schema_three"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err := postSubjectVersion(db, nil, "three", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)

	// create a new schema that references nested self
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_four",
  "fields": [
    {"name": "field1", "type": {"name":"nestedSelf", "type": "record", "fields": [{"name": "field1", "type": "schema_four"}]}}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "four", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), resp.ID)

	// create a new schema that references nested self and redefines
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_five",
  "fields": [
    {"name": "field1", "type": {"name":"schema_five", "type": "record", "fields": [{"name": "field1", "type": "schema_five"}]}}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "five", requestPostSubject)
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)
}

func TestPostSubjectVersionAvroOverwriteReferences(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	requestPostSubject := &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "long"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err := postSubjectVersion(db, nil, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.ID)

	// create a new schema that references one, overwriting name
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "schema_one"}
  ]
}
	`,
		References: []SubjectReference{
			{
				Name:    "schema_one",
				Subject: "one",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "five", requestPostSubject)
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// create a new schema that references one, overwriting name but namespaced
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "namespace": "myNamespace",
  "name": "schema_one",
  "fields": [
    {"name": "field1", "type": "schema_one"}
  ]
}
	`,
		References: []SubjectReference{
			{
				Name:    "schema_one",
				Subject: "one",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "five", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), resp.ID)

	// create a new schema that references one, overwriting name nesting
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_six",
  "fields": [
    {"name": "field1", "type": {"name":"schema_one", "type": "record", "fields": [{"name": "nestedField1", "type": "schema_one"}]}}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_one",
				Subject: "one",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "six", requestPostSubject)
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// create a new schema that references one, overwriting name nesting with namespace
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_six",
  "fields": [
    {"name": "field1", "type": {"namespace": "myNamespace", "name":"schema_one", "type": "record", "fields": [{"name": "nestedField1", "type": "schema_one"}]}}
  ]
}
`,
		References: []SubjectReference{
			{
				Name:    "schema_one",
				Subject: "one",
				Version: int32(1),
			},
		},
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "six", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, int32(3), resp.ID)
}

func TestPostSubjectVersionNewVersionDifferentSchemaTypes(t *testing.T) {
	// TODO: create a avro subject then try and create a new version that is a json schema type
}

func TestPostSubjectVersionReferenceDifferentSchemaTypes(t *testing.T) {
	// TODO: create a avro subject then try and create a new subject that is of a different type that references the first subject
}
