package subjects

import (
	"errors"
	"fmt"
	"net/http"

	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"gorm.io/gorm"
)

func getSubjectVersionReferencedBy(db *gorm.DB, subjectName string, version string) (*ResponseGetSubjectVersionReferencedBy, error) {
	response := ResponseGetSubjectVersionReferencedBy{}

	err := db.Transaction(func(tx *gorm.DB) error {
		subject, err := getSubjectByName(tx, subjectName)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("subject not found"))
			}
			return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
		}

		versionModel, err := getSubjectVersionBySubjectID(tx, subject.ID, version)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("version not found"))
			}
			return fmt.Errorf("error finding version %s for subject %s: %w", version, subjectName, err)
		}

		schemaReferences := make([]dbModels.SchemaReference, 0)
		err = tx.Clauses(forceIndexHint("idx_subject_version_id")).Joins("Schema").Where("schema_references.subject_version_id = ?", versionModel.ID).Find(&schemaReferences).Error
		if err != nil {
			return fmt.Errorf("error finding references: %w", err)
		}

		for _, reference := range schemaReferences {
			response = append(response, reference.Schema.GlobalID)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &response, err
}
