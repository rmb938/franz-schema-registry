package subjects

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-chi/render"
	"github.com/google/uuid"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"github.com/rmb938/franz-schema-registry/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestPostSubject(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// try to post on empty db
	resp, err := postSubject(db, "unknown", &RequestPostSubject{})
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// try to post bad schema type
	resp, err = postSubject(db, "unknown", &RequestPostSubject{
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
	resp, err = postSubject(db, "unknown", &RequestPostSubject{
		SchemaType: schemas.SchemaTypeAvro,
	})
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	subjectOne := &dbModels.Subject{
		ID:            uuid.New(),
		Name:          "one",
		Compatibility: dbModels.SubjectCompatibilityBackward,
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(subjectOne).Error; err != nil {
			return fmt.Errorf("error creating subjectOne: %w", err)
		}

		for i := 1; i <= 5; i++ {
			// tx because sqlite doesn't allow multiple write transactions at once
			globalID, err := dbModels.NextSequenceID(tx, dbModels.SequenceNameSchemaIDs)
			if err != nil {
				return fmt.Errorf("error getting next sequence id: %w", err)
			}

			schemaString := `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field_%d", "type": "long"}
  ]
}
`
			schemaString = fmt.Sprintf(schemaString, i)

			hash, err := calculateSchemaHash(schemaString, nil)
			if err != nil {
				return err
			}

			schema := &dbModels.Schema{
				ID:         uuid.New(),
				GlobalID:   int32(globalID),
				Schema:     schemaString,
				Hash:       hash,
				SchemaType: dbModels.SchemaTypeAvro,
			}
			if err := tx.Create(schema).Error; err != nil {
				return fmt.Errorf("error creating schema: %w", err)
			}

			subjectVersion := &dbModels.SubjectVersion{
				ID:        uuid.New(),
				SubjectID: subjectOne.ID,
				SchemaID:  schema.ID,
				Version:   int32(i),
			}
			if err := tx.Create(subjectVersion).Error; err != nil {
				return fmt.Errorf("error creating subjectOne version: %w", err)
			}
		}

		return nil
	})
	assert.NoError(t, err)

	subjectTwo := &dbModels.Subject{
		ID:            uuid.New(),
		Name:          "two",
		Compatibility: dbModels.SubjectCompatibilityBackward,
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(subjectTwo).Error; err != nil {
			return fmt.Errorf("error creating subjectTwo: %w", err)
		}

		for i := 1; i <= 5; i++ {
			// tx because sqlite doesn't allow multiple write transactions at once
			globalID, err := dbModels.NextSequenceID(tx, dbModels.SequenceNameSchemaIDs)
			if err != nil {
				return fmt.Errorf("error getting next sequence id: %w", err)
			}

			schemaString := `
{
  "type": "record",
  "name": "schema_two",
  "fields": [
    {"name": "field_%d", "type": "long"}
  ]
}
`
			schemaString = fmt.Sprintf(schemaString, i)

			hash, err := calculateSchemaHash(schemaString, nil)
			if err != nil {
				return err
			}

			schema := &dbModels.Schema{
				ID:         uuid.New(),
				GlobalID:   int32(globalID),
				Schema:     schemaString,
				Hash:       hash,
				SchemaType: dbModels.SchemaTypeAvro,
			}
			if err := tx.Create(schema).Error; err != nil {
				return fmt.Errorf("error creating schema: %w", err)
			}

			subjectVersion := &dbModels.SubjectVersion{
				ID:        uuid.New(),
				SubjectID: subjectTwo.ID,
				SchemaID:  schema.ID,
				Version:   int32(i),
			}
			if err := tx.Create(subjectVersion).Error; err != nil {
				return fmt.Errorf("error creating subjectTwo version: %w", err)
			}
		}

		return nil
	})
	assert.NoError(t, err)

	// post subject invalid schema
	resp, err = postSubject(db, "one", &RequestPostSubject{
		Schema: "bad",
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
	resp, err = postSubject(db, "one", &RequestPostSubject{
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

	// post subject good references but schema not found
	resp, err = postSubject(db, "two", &RequestPostSubject{
		Schema: `{"type": "string"}`,
		References: []SubjectReference{
			{
				Name:    "ref",
				Subject: "one",
				Version: int32(1),
			},
		},
	})
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40403, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// post subject schema found
	requestPostSubject := &RequestPostSubject{
		Schema: `
{
  "type": "record",
  "name": "schema_one",
  "fields": [
    {"name": "field_1", "type": "long"}
  ]
}
`,
	}
	assert.NoError(t, requestPostSubject.Bind(nil))
	resp, err = postSubject(db, "one", requestPostSubject)
	assert.NoError(t, err)
	assert.Equal(t, "one", resp.Subject)
	assert.Equal(t, int32(1), resp.ID)
	assert.Equal(t, int32(1), resp.Version)
	assert.Equal(t, requestPostSubject.Schema, resp.Schema)
}
