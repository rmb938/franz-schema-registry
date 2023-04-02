package subjects

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/rmb938/franz-schema-registry/pkg/database/migrations"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestGetSubjects(t *testing.T) {
	// setup DB
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		DisableNestedTransaction: true,
	})
	assert.NoError(t, err)
	assert.NoError(t, migrations.RunMigrations(db))

	// get subjects on empty db
	resp, err := getSubjects(db, false)
	assert.NoError(t, err)
	assert.Empty(t, resp)

	// insert some subjects
	subjects := []string{
		"one",
		"two",
		"three",
	}
	err = db.Transaction(func(tx *gorm.DB) error {

		for _, subjectName := range subjects {
			subject := &dbModels.Subject{
				ID:            uuid.New(),
				Name:          subjectName,
				Compatibility: dbModels.SubjectCompatibilityBackward,
			}
			if err := tx.Create(subject).Error; err != nil {
				return fmt.Errorf("error creating subject: %s: %w", subject.Name, err)
			}
		}

		return nil
	})
	assert.NoError(t, err)

	// get subjects again and make sure they match
	resp, err = getSubjects(db, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, subjects, *resp)

	// soft delete subjects
	err = db.Transaction(func(tx *gorm.DB) error {
		return tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&dbModels.Subject{}).Error
	})
	assert.NoError(t, err)

	// getting subjects should be empty
	resp, err = getSubjects(db, false)
	assert.NoError(t, err)
	assert.Empty(t, resp)

	// getting subjects with include deleted should not be empty
	resp, err = getSubjects(db, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, subjects, *resp)
}
