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
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestGetSubjectVersions(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// try to get versions on empty db
	resp, err := getSubjectVersions(db, "unknown", false)
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

		// tx because sqlite doesn't allow multiple write transactions at once
		globalID, err := dbModels.NextSequenceID(tx, dbModels.SequenceNameSchemaIDs)
		if err != nil {
			return fmt.Errorf("error getting next sequence id: %w", err)
		}

		schema := &dbModels.Schema{
			ID:         uuid.New(),
			GlobalID:   int32(globalID),
			Schema:     "", // schema and hash doesn't matter for this test
			Hash:       "",
			SchemaType: dbModels.SchemaTypeAvro,
		}
		if err := tx.Create(schema).Error; err != nil {
			return fmt.Errorf("error creating schema: %w", err)
		}

		subjectVersion := &dbModels.SubjectVersion{
			ID:        uuid.New(),
			SubjectID: subject.ID,
			SchemaID:  schema.ID,
			Version:   1,
		}
		if err := tx.Create(subjectVersion).Error; err != nil {
			return fmt.Errorf("error creating subject version: %w", err)
		}

		return nil
	})
	assert.NoError(t, err)

	// try to get versions again
	resp, err = getSubjectVersions(db, "one", false)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp)
	assert.ElementsMatch(t, []int32{1}, *resp)

	// soft delete
	err = db.Transaction(func(tx *gorm.DB) error {
		return tx.Where(&dbModels.SubjectVersion{Version: 1}).Delete(&dbModels.SubjectVersion{}).Error
	})
	assert.NoError(t, err)

	// subject versions should not be found
	resp, err = getSubjectVersions(db, "one", false)
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// subject versions should have soft deleted item
	resp, err = getSubjectVersions(db, "one", true)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp)
	assert.ElementsMatch(t, []int32{1}, *resp)
}
