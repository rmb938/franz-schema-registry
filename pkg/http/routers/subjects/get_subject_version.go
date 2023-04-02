package subjects

import (
	"errors"
	"fmt"
	"net/http"

	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"github.com/rmb938/franz-schema-registry/pkg/schemas"
	"gorm.io/gorm"
)

func getSubjectVersion(db *gorm.DB, subjectName string, version string) (*ResponseGetSubjectVersion, error) {

	response := &ResponseGetSubjectVersion{}

	err := db.Transaction(func(tx *gorm.DB) error {
		subject, err := getSubjectByName(tx, subjectName, false)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("subject not found"))
			}
			return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
		}

		versionModel, err := getSubjectVersionBySubjectID(tx, subject.ID, version, false)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40402, fmt.Errorf("version not found"))
			}
			return fmt.Errorf("error finding version %s for subject %s: %w", version, subjectName, err)
		}

		schema := &dbModels.Schema{}
		err = tx.Where("id = ?", versionModel.SchemaID).First(schema).Error
		if err != nil {
			return fmt.Errorf("error finding schema for version %s for subject %s: %w", version, subjectName, err)
		}

		response.Subject = subjectName
		response.ID = schema.GlobalID
		response.Version = versionModel.Version
		response.SchemaType = schemas.SchemaType(schema.SchemaType)
		response.Schema = schema.Schema

		if response.SchemaType == schemas.SchemaTypeAvro {
			// set to empty string when avro for compatibility
			response.SchemaType = ""
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}
