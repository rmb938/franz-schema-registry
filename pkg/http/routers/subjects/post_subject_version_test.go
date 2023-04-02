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

func TestPostSubjectVersion(t *testing.T) {
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
	resp, err = postSubjectVersion(db, nil, "one", requestPostSubject)
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
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 409, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusConflict, w.Result().StatusCode)

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
	assert.Equal(t, int32(3), resp.ID)

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
	resp, err = postSubjectVersion(db, nil, "two", requestPostSubject)
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// create a new schema that references self
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
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42201, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// create a new schema that references nested self
	requestPostSubject = &RequestPostSubjectVersion{
		Schema: `
{
  "type": "record",
  "name": "schema_two",
  "fields": [
    {"name": "field1", "type": {"name":"nestedSelf", "type": "record", "fields": [{"name": "field1", "type": "schema_two"}]}}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubjectVersion(db, nil, "two", requestPostSubject)
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, "", apiError.Error())
	assert.Equal(t, 42201, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)
}
