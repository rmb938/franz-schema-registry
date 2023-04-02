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

func TestGetSubjectVersionReferencedBy(t *testing.T) {
	db, dbFile := TempDatabase(t)
	defer func() {
		err := os.Remove(dbFile)
		if err != nil {
			t.Error("db file remove error:", err)
		}
	}()

	// try to get version on empty db
	resp, err := getSubjectVersionReferencedBy(db, "unknown", "1")
	apiError := &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40401, apiError.ErrorCode)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
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

			schema := &dbModels.Schema{
				ID:         uuid.New(),
				GlobalID:   int32(globalID),
				Schema:     "", // schema and hash doesn't matter for this test
				Hash:       fmt.Sprintf("one-%d", i),
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

			schema := &dbModels.Schema{
				ID:         uuid.New(),
				GlobalID:   int32(globalID),
				Schema:     "", // schema and hash doesn't matter for this test
				Hash:       fmt.Sprintf("two-%d", i),
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

			subjectVersionFromOne := &dbModels.SubjectVersion{}
			err = db.Where(&dbModels.SubjectVersion{SubjectID: subjectOne.ID, Version: int32(i)}).First(subjectVersionFromOne).Error
			if err != nil {
				return fmt.Errorf("error getting subject version from one: %w", err)
			}

			schemaReference := &dbModels.SchemaReference{
				ID:               uuid.New(),
				SchemaID:         schema.ID,
				SubjectVersionID: subjectVersionFromOne.ID,
				Name:             "one",
			}
			if err := tx.Create(schemaReference).Error; err != nil {
				return fmt.Errorf("error creating subjectTwo version reference: %w", err)
			}
		}

		return nil
	})
	assert.NoError(t, err)

	// get references for one unknown version
	resp, err = getSubjectVersionReferencedBy(db, "one", "1000")
	apiError = &routers.APIError{}
	assert.ErrorAs(t, err, &apiError)
	assert.Nil(t, resp)
	assert.Equal(t, 40402, apiError.ErrorCode)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	w = httptest.NewRecorder()
	assert.NoError(t, render.Render(w, req, apiError))
	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

	// get references for one version one
	resp, err = getSubjectVersionReferencedBy(db, "one", "1")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.ElementsMatch(t, []int32{6}, *resp)

	// get references for two version one
	resp, err = getSubjectVersionReferencedBy(db, "two", "1")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Empty(t, *resp)
}
