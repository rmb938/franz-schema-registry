package subjects

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/go-chi/render"
	"github.com/google/uuid"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"github.com/rmb938/franz-schema-registry/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestGetSubjectVersionTest(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// try to get version on empty db
	resp, err := getSubjectVersion(db, "unknown", "1")
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// insert subject, schema & version
	err = db.Transaction(func(tx *gorm.DB) error {
		subject := &dbModels.Subject{
			ID:            uuid.New(),
			Name:          "one",
			Compatibility: dbModels.SubjectCompatibilityBackward,
		}
		if err := tx.Create(subject).Error; err != nil {
			return fmt.Errorf("error creating subject: %w", err)
		}

		for i := 1; i <= 5; i++ {
			// tx because sqlite doesn't allow multiple write transactions at once
			globalID, err := dbModels.NextSequenceID(tx, dbModels.SequenceNameSchemaIDs)
			if err != nil {
				return fmt.Errorf("error getting next sequence id: %w", err)
			}

			schema := &dbModels.Schema{
				ID:         uuid.New(),
				GlobalID:   int32(globalID),
				Schema:     "", // schema and hash doesn't matter for this test
				Hash:       strconv.Itoa(i),
				SchemaType: dbModels.SchemaTypeAvro,
			}
			if err := tx.Create(schema).Error; err != nil {
				return fmt.Errorf("error creating schema: %w", err)
			}

			subjectVersion := &dbModels.SubjectVersion{
				ID:        uuid.New(),
				SubjectID: subject.ID,
				SchemaID:  schema.ID,
				Version:   int32(i),
			}
			if err := tx.Create(subjectVersion).Error; err != nil {
				return fmt.Errorf("error creating subject version: %w", err)
			}
		}

		return nil
	})
	assert.NoError(t, err)

	// get unknown version
	resp, err = getSubjectVersion(db, "one", "1000")
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40402, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// get bad version
	resp, err = getSubjectVersion(db, "one", "a")
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 42202, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Result().StatusCode)

	// get latest version
	resp, err = getSubjectVersion(db, "one", "latest")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "one", resp.Subject)
	assert.Equal(t, int32(5), resp.ID)
	assert.Equal(t, int32(5), resp.Version)
	assert.Equal(t, schemas.SchemaType(""), resp.SchemaType)

	// get -1 version
	resp, err = getSubjectVersion(db, "one", "-1")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "one", resp.Subject)
	assert.Equal(t, int32(5), resp.ID)
	assert.Equal(t, int32(5), resp.Version)
	assert.Equal(t, schemas.SchemaType(""), resp.SchemaType)

	// get specific version
	resp, err = getSubjectVersion(db, "one", "3")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "one", resp.Subject)
	assert.Equal(t, int32(3), resp.ID)
	assert.Equal(t, int32(3), resp.Version)
	assert.Equal(t, schemas.SchemaType(""), resp.SchemaType)
}
