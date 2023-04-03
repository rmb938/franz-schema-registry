package subjects

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/google/uuid"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"github.com/rmb938/franz-schema-registry/pkg/schemas"
	"gorm.io/gorm"
)

func getSubjectVersionsReferencedBySubjectNameAndVersion(tx *gorm.DB, referenceName string, subjectName string, version int32, schemaType dbModels.SchemaType) ([]string, map[string]dbModels.SubjectVersion, error) {
	referenceNames := make([]string, 0)
	subjectVersions := make(map[string]dbModels.SubjectVersion)

	subjectVersion := &dbModels.SubjectVersion{}
	err := tx.Joins("Schema").Joins("Subject").Where("\"Subject\".\"name\" = ? AND subject_versions.version = ?", subjectName, version).First(subjectVersion).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, routers.NewAPIError(http.StatusNotFound, 40402, fmt.Errorf("no schema reference found for subject %s and version %d", subjectName, version))
		}
		return nil, nil, fmt.Errorf("error finding reference to subject %s and version %d: %w", subjectName, version, err)
	}

	if schemaType != subjectVersion.Schema.SchemaType {
		return nil, nil, routers.NewAPIError(http.StatusConflict, 40901, fmt.Errorf("cannot reference schema with a different type"))
	}

	subReferences, err := getSchemaReferencesReferencedBySchemaID(tx, subjectVersion.Schema.ID, 0)
	if err != nil {
		apiError := &routers.APIError{}
		if errors.As(err, &apiError) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("error getting subreferences for schema %d: %w", subjectVersion.Schema.GlobalID, err)
	}

	for _, reference := range subReferences {
		subjectVersions[reference.Name] = reference.SubjectVersion
		referenceNames = append(referenceNames, reference.Name)
	}

	subjectVersions[referenceName] = *subjectVersion
	referenceNames = append(referenceNames, referenceName)

	return referenceNames, subjectVersions, nil
}

func getSchemaReferencesReferencedBySchemaID(tx *gorm.DB, schemaID uuid.UUID, recursions int) ([]dbModels.SchemaReference, error) {
	if recursions >= 5 {
		// we need something here to stop long reference chains as the longer the chain the more db queries and the longer it'll take
		// too long of a chain will eventually take things down
		// TODO: make this configurable with a sane default
		return nil, routers.NewAPIError(http.StatusConflict, 40902, fmt.Errorf("hit recursive schema limit, reference chain is too deep"))
	}

	schemaReferences := make([]dbModels.SchemaReference, 0)
	err := tx.Clauses(forceIndexHint("idx_schema_id")).Joins("SubjectVersion").Joins("SubjectVersion.Schema").
		Where("schema_references.schema_id = ?", schemaID).
		Find(&schemaReferences).Error
	if err != nil {
		return nil, fmt.Errorf("error getting existing schema version references: %w", err)
	}

	totalSchemaReferences := make([]dbModels.SchemaReference, 0)

	for _, schemaReference := range schemaReferences {
		subReferences, err := getSchemaReferencesReferencedBySchemaID(tx, schemaReference.SubjectVersion.Schema.ID, recursions+1)
		if err != nil {
			apiError := &routers.APIError{}
			if errors.As(err, &apiError) {
				return nil, err
			}
			return nil, fmt.Errorf("error getting subreferences for schema %d: %w", schemaReference.SubjectVersion.Schema.GlobalID, err)
		}

		totalSchemaReferences = append(totalSchemaReferences, subReferences...)
		totalSchemaReferences = append(totalSchemaReferences, schemaReference)
	}

	// TODO: we probably also want to limit the size of totalSchemaReferences for the same reasons we limit the reference chain size

	return totalSchemaReferences, nil
}

func postSubjectVersion(db *gorm.DB, nextSequenceTx *gorm.DB, subjectName string, data *RequestPostSubjectVersion) (*ResponsePostSubjectVersion, error) {
	resp := &ResponsePostSubjectVersion{}

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
		if nextSequenceTx == nil {
			nextSequenceTx = tx
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

		parsedSchema, err := schemas.ParseSchema(data.Schema, schemaType, newRawReferences)
		if err != nil {
			return routers.NewAPIError(http.StatusUnprocessableEntity, 42201, fmt.Errorf("error parsing schema: %w", err))
		}

		subject, err := getSubjectByName(tx, subjectName, true)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) == false {
				return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
			}
			subject = nil
		}

		// if subject is nil, create it
		if subject == nil {
			subject = &dbModels.Subject{
				ID:            uuid.New(),
				Name:          subjectName,
				Compatibility: dbModels.SubjectCompatibilityBackward,
			}
			if err := tx.Create(subject).Error; err != nil {
				return fmt.Errorf("error creating subject: %s: %w", subjectName, err)
			}
		}

		// subject was soft deleted and now we want it back
		if subject.DeletedAt.Valid {
			err := tx.Unscoped().Model(subject).Update("deleted_at", nil).Error
			if err != nil {
				return fmt.Errorf("error unsoft deleting subject: %s: %w", subjectName, err)
			}
		}

		// checking compatibility
		if subject.Compatibility != dbModels.SubjectCompatibilityNone {

			existingSchemaVersions := make([]dbModels.SubjectVersion, 0)
			query := tx.Joins("Schema").Where("subject_versions.subject_id = ?", subject.ID).Order("subject_versions.version desc").Find(&existingSchemaVersions)

			// check if we are transitive
			if !strings.HasSuffix(string(subject.Compatibility), "_TRANSITIVE") {
				// not transitive so we only need the first one
				query = query.Limit(1)
			} else {
				// we are transitive, this is most likely a very expensive operation, so it's probably not a good idea to do
				// this query could return tons of rows and require tons of comparisons
				// we probably could limit this impact by having a configurable maximum versions per subject
				query = query.Limit(-1)
			}

			err := query.Find(&existingSchemaVersions).Error
			if err != nil {
				return fmt.Errorf("error finding existing schemas for compatibility checking: %w", err)
			}

			var existingParsedSchemas []schemas.ParsedSchema
			for _, existingSchemaVersion := range existingSchemaVersions {
				references := make([]string, 0)

				// if it exists it means the original schema passed recursion validation
				// so let's set it to -1 to offset any weirdness
				schemaReferences, err := getSchemaReferencesReferencedBySchemaID(tx, existingSchemaVersion.Schema.ID, -1)
				if err != nil {
					return err
				}

				for _, schemaReference := range schemaReferences {
					references = append(references, schemaReference.SubjectVersion.Schema.Schema)
				}

				existingParsedSchema, err := schemas.ParseSchema(existingSchemaVersion.Schema.Schema, schemaType, references)
				if err != nil {
					return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error parsing existing: %w", err))
				}

				existingParsedSchemas = append(existingParsedSchemas, existingParsedSchema)
			}

			compatible := true
			switch subject.Compatibility {
			case dbModels.SubjectCompatibilityBackward:
				fallthrough
			case dbModels.SubjectCompatibilityBackwardTransitive:
				for _, existingParsedSchema := range existingParsedSchemas {
					isBackwardsCompatible, err := parsedSchema.IsBackwardsCompatible(existingParsedSchema)
					if err != nil {
						return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error checking compatibility: %w", err))
					}

					if isBackwardsCompatible == false {
						compatible = false
						break
					}
				}
				break
			case dbModels.SubjectCompatibilityForward:
				fallthrough
			case dbModels.SubjectCompatibilityForwardTransitive:
				for _, existingParsedSchema := range existingParsedSchemas {
					isBackwardsCompatible, err := existingParsedSchema.IsBackwardsCompatible(parsedSchema)
					if err != nil {
						return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error checking compatibility: %w", err))
					}

					if isBackwardsCompatible == false {
						compatible = false
						break
					}
				}
				break
			case dbModels.SubjectCompatibilityFull:
				fallthrough
			case dbModels.SubjectCompatibilityFullTransitive:
				for _, existingParsedSchema := range existingParsedSchemas {
					isBackwardsCompatible, err := parsedSchema.IsBackwardsCompatible(existingParsedSchema)
					if err != nil {
						return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error checking compatibility: %w", err))
					}

					if isBackwardsCompatible == false {
						compatible = false
						break
					}

					isBackwardsCompatible, err = existingParsedSchema.IsBackwardsCompatible(parsedSchema)
					if err != nil {
						return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error checking compatibility: %w", err))
					}

					if isBackwardsCompatible == false {
						compatible = false
						break
					}
				}
				break
			}

			if compatible == false {
				return routers.NewAPIError(http.StatusConflict, http.StatusConflict, fmt.Errorf("schema is incompatible with an earlier schema"))
			}
		}

		schema := &dbModels.Schema{}
		err = tx.Clauses(forceIndexHint("idx_schemas_hash")).
			Where("hash = ?", data.calculatedHash).First(schema).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) == false {
				return fmt.Errorf("error finding schema for subject %s: %w", subjectName, err)
			}
			schema = nil
		}

		// if schema is nil, create it
		if schema == nil {
			// get the next sequence, use the normal db as we don't want a nested transaction
			nextId, err := dbModels.NextSequenceID(nextSequenceTx, dbModels.SequenceNameSchemaIDs)
			if err != nil {
				return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error generating next schema id: %w", err))
			}

			if nextId > math.MaxInt32 {
				return routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("too many schemas registered next schema id is greater than int32"))
			}

			// create it
			schema = &dbModels.Schema{
				ID:         uuid.New(),
				GlobalID:   int32(nextId),
				Schema:     data.Schema,
				Hash:       data.calculatedHash,
				SchemaType: dbSchemaType,
			}
			if err := tx.Create(schema).Error; err != nil {
				return fmt.Errorf("error creating schema for subject: %s: %w", subjectName, err)
			}

			for _, reference := range data.References {
				dbReference := &dbModels.SchemaReference{
					ID:               uuid.New(),
					SchemaID:         schema.ID,                                   // The schema that we are creating
					SubjectVersionID: subjectVersionReferences[reference.Name].ID, // The subject version that we are referencing
					Name:             reference.Name,
				}
				if err := tx.Create(dbReference).Error; err != nil {
					return fmt.Errorf("error creating schema reference for subject: %s: %w", subjectName, err)
				}
			}
		}
		resp.ID = schema.GlobalID

		subjectVersion := &dbModels.SubjectVersion{}
		err = tx.Clauses(forceIndexHint("idx_subject_id_schema_id")).
			Where("subject_id = ? AND schema_id = ?", subject.ID, schema.ID).First(subjectVersion).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) == false {
				return fmt.Errorf("error finding subject version for subject %s: %w", subjectName, err)
			}
			subjectVersion = nil
		}

		// if subject version is nil create it
		if subjectVersion == nil {
			latestVersion := &dbModels.SubjectVersion{}
			latestVersionNum := int32(1)
			// unscoped because we need to include soft deleted and skip that version if it's soft deleted
			err = tx.Unscoped().Clauses(forceIndexHint("idx_subject_versions_subject_id")).
				Order("version desc").Where("subject_id = ?", subject.ID).First(latestVersion).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) == false {
					return fmt.Errorf("error finding latest version for subject %s: %w", subjectName, err)
				}
				latestVersion = nil
			}

			if latestVersion != nil {
				latestVersionNum = latestVersion.Version + 1

				latestSchema := &dbModels.Schema{}
				err = tx.Where("id = ?", latestVersion.SchemaID).First(latestSchema).Error
				if err != nil {
					return fmt.Errorf("error finding schema for subject latest version %s: %w", subjectName, err)
				}

				// TODO: match the error message from confluent schema registry
				// TODO: this takes into account deleted versions, does confluent do that?
				if latestSchema.SchemaType != schema.SchemaType {
					return routers.NewAPIError(http.StatusConflict, http.StatusConflict, fmt.Errorf("cannot add version of a different schema type"))
				}
			}

			subjectVersion = &dbModels.SubjectVersion{
				ID:        uuid.New(),
				SubjectID: subject.ID,
				SchemaID:  schema.ID,
				Version:   latestVersionNum,
			}
			if err := tx.Create(subjectVersion).Error; err != nil {
				return fmt.Errorf("error creating version for subject: %s: %w", subjectName, err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}
