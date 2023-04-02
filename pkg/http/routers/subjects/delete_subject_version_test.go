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
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestDeleteSubjectVersion(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// try to delete subject version empty db
	resp, err := deleteSubjectVersion(db, "unknown", "1", false)
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodDelete, "/", nil)
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

	// try and delete a version that doesn't exist
	resp, err = deleteSubjectVersion(db, "one", "1000", false)
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodDelete, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// try and hard delete before soft delete
	resp, err = deleteSubjectVersion(db, "one", "1", true)
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40901, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodDelete, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusConflict, w.Result().StatusCode)

	// hard delete latest version
	resp, err = deleteSubjectVersion(db, "one", "latest", true)
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40001, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodDelete, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)

	// hard delete -1 version
	resp, err = deleteSubjectVersion(db, "one", "-1", true)
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40001, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodDelete, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)

	// delete latest version
	resp, err = deleteSubjectVersion(db, "one", "latest", false)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, ResponseDeleteSubjectVersion(5), *resp)
	subjectVersion := &dbModels.SubjectVersion{}
	err = db.Unscoped().Where(&dbModels.SubjectVersion{Version: 5}).First(subjectVersion).Error
	assert.NoError(t, err)
	assert.True(t, subjectVersion.DeletedAt.Valid)

	// delete -1 version
	resp, err = deleteSubjectVersion(db, "one", "-1", false)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, ResponseDeleteSubjectVersion(4), *resp)
	subjectVersion = &dbModels.SubjectVersion{}
	err = db.Unscoped().Where(&dbModels.SubjectVersion{Version: 4}).First(subjectVersion).Error
	assert.NoError(t, err)
	assert.True(t, subjectVersion.DeletedAt.Valid)

	// soft delete 3
	resp, err = deleteSubjectVersion(db, "one", "3", false)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, ResponseDeleteSubjectVersion(3), *resp)
	subjectVersion = &dbModels.SubjectVersion{}
	err = db.Unscoped().Where(&dbModels.SubjectVersion{Version: 3}).First(subjectVersion).Error
	assert.NoError(t, err)
	assert.True(t, subjectVersion.DeletedAt.Valid)

	// hard delete 3
	resp, err = deleteSubjectVersion(db, "one", "3", true)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, ResponseDeleteSubjectVersion(3), *resp)
	err = db.Unscoped().Where(&dbModels.SubjectVersion{Version: 3}).First(&dbModels.SubjectVersion{}).Error
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
}
