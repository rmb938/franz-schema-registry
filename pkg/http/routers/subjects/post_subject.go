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

func postSubject(db *gorm.DB, subjectName string, data *RequestPostSubject) (*ResponsePostSubject, error) {
	resp := &ResponsePostSubject{}

	schemaType := schemas.SchemaTypeAvro
	dbSchemaType := dbModels.SchemaTypeAvro
	if len(data.SchemaType) > 0 {
		schemaType = data.SchemaType
		switch data.SchemaType {
		case schemas.SchemaTypeAvro:
			dbSchemaType = dbModels.SchemaTypeAvro
		// TODO: uncomment once these other types are supported
		// case SchemaTypeJSON:
		// 	dbSchemaType = dbModels.SchemaTypeJSON
		// case SchemaTypeProtobuf:
		// 	dbSchemaType = dbModels.SchemaTypeProtobuf
		default:
			return nil, routers.NewAPIError(http.StatusBadRequest, http.StatusBadRequest, fmt.Errorf("unknown schema type: %s", data.SchemaType))
		}
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		subject, err := getSubjectByName(tx, subjectName, false)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("subject not found"))
			}
			return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
		}

		subjectVersionReferences := make(map[string]dbModels.SubjectVersion)
		newRawReferences := make([]string, 0)
		for _, reference := range data.References {
			referencesSlice, referencesMap, err := getSubjectVersionsReferencedBySubjectNameAndVersion(tx, reference.Name, reference.Subject, reference.Version, dbSchemaType)
			if err != nil {
				return err
			}

			for _, name := range referencesSlice {
				subjectVersionReferences[name] = referencesMap[name]
				newRawReferences = append(newRawReferences, referencesMap[name].Schema.Schema)
			}
		}

		_, err = schemas.ParseSchema(data.Schema, schemaType, newRawReferences)
		if err != nil {
			return routers.NewAPIError(http.StatusUnprocessableEntity, 42201, fmt.Errorf("error parsing schema: %w", err))
		}

		schema := &dbModels.Schema{}
		err = tx.Clauses(forceIndexHint("idx_schemas_hash")).
			Where("hash = ? AND schema_type = ?", data.calculatedHash, schemaType).First(schema).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40403, fmt.Errorf("schema not found"))
			}
			return fmt.Errorf("error finding schema for subject %s: %w", subjectName, err)
		}

		subjectVersion := &dbModels.SubjectVersion{}
		err = tx.Clauses(forceIndexHint("idx_subject_id_schema_id")).
			Where("subject_id = ? AND schema_id = ?", subject.ID, schema.ID).First(subjectVersion).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40403, fmt.Errorf("schema not found"))
			}
			return fmt.Errorf("error finding subject version for subject %s: %w", subjectName, err)
		}

		resp.Subject = subject.Name
		resp.ID = schema.GlobalID
		resp.Version = subjectVersion.Version
		resp.Schema = schema.Schema

		return nil
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}
