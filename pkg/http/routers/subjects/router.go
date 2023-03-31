package subjects

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/google/uuid"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/schemas"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/hints"
)

func forceIndexHint(index string) hints.Hints {
	forceIndexHint := hints.CommentBefore("where", fmt.Sprintf("FORCE_INDEX = %s", index))
	forceIndexHint.Prefix = "/*@ "
	return forceIndexHint
}

func NewRouter(db *gorm.DB) *chi.Mux {
	chiRouter := chi.NewRouter()

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects
	chiRouter.Get("/", func(writer http.ResponseWriter, request *http.Request) {
		var subjects []dbModels.Subject

		// Whether to included soft deleted subjects
		deletedRaw := request.URL.Query().Get("deleted")
		deleted, _ := strconv.ParseBool(deletedRaw)

		subjectsDB := db
		if deleted {
			subjectsDB = subjectsDB.Unscoped()
		}

		result := subjectsDB.Find(&subjects)
		if result.Error != nil {
			render.Status(request, http.StatusInternalServerError)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": 50001,
				"message":    fmt.Sprintf("error listing subjects from database: %s", result.Error),
			})
			return
		}

		subjectList := make([]string, len(subjects))
		for index, subject := range subjects {
			subjectList[index] = subject.Name
		}

		render.Status(request, http.StatusOK)
		render.JSON(writer, request, subjectList)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions
	chiRouter.Get("/{subject}/versions", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")

		// Whether to included soft deleted versions
		deletedRaw := request.URL.Query().Get("deleted")
		deleted, _ := strconv.ParseBool(deletedRaw)

		var subjectVersions []dbModels.SubjectVersion

		subjectVersionsDB := db
		if deleted {
			subjectVersionsDB = subjectVersionsDB.Unscoped()
		}
		err := subjectVersionsDB.Model(&dbModels.SubjectVersion{}).
			Clauses(forceIndexHint("idx_subjects_name")).
			Joins("JOIN subjects ON subjects.id = subject_versions.subject_id").
			Where("subjects.name = ? AND subjects.deleted_at is NULL", subjectName).
			Order("subject_versions.version asc").Find(&subjectVersions).Error

		if err != nil {
			render.Status(request, http.StatusInternalServerError)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": 50001,
				"message":    fmt.Sprintf("error listing subject versions: %s", err),
			})
			return
		}

		if len(subjectVersions) == 0 {
			// TODO: no versions, so we should check if the subject actually exists, if it does return a empty list

			render.Status(request, http.StatusNotFound)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": 40401,
				"message":    fmt.Sprintf("subject not found"),
			})
			return
		}

		subjectVersionIDs := make([]int32, len(subjectVersions))
		for index, subjectVersion := range subjectVersions {
			subjectVersionIDs[index] = subjectVersion.Version
		}

		render.Status(request, http.StatusOK)
		render.JSON(writer, request, subjectVersionIDs)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)
	chiRouter.Delete("/{subject}", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")

		permanentRaw := request.URL.Query().Get("permanent")
		permanent, _ := strconv.ParseBool(permanentRaw)

		errorCode := http.StatusInternalServerError
		var errorResponse map[string]interface{}

		var subjectVersions []dbModels.SubjectVersion

		err := db.Transaction(func(tx *gorm.DB) error {
			subject := &dbModels.Subject{}
			err := tx.Unscoped().Clauses(forceIndexHint("idx_subjects_name")).
				Where("name = ?", subjectName).First(subject).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40401,
						"message":    "subject not found",
					}
					return fmt.Errorf("subject not found")
				}
				return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
			}

			if permanent && subject.DeletedAt.Valid == false {
				errorCode = http.StatusNotFound
				errorResponse = map[string]interface{}{
					"error_code": http.StatusConflict,
					"message":    "must soft delete subject before permanently deleting",
				}
				return fmt.Errorf("must soft delete first")
			}

			deleteVersionsTx := tx
			if permanent {
				deleteVersionsTx = deleteVersionsTx.Unscoped()
			}

			err = tx.Clauses(clause.Returning{}).Where("subject_id = ?", subject.ID).Delete(&subjectVersions).Error
			if err != nil {
				return fmt.Errorf("error deleting subject versions: %w", err)
			}

			deleteSubjectTx := tx
			if permanent {
				deleteSubjectTx = deleteVersionsTx.Unscoped()
			}

			err = deleteSubjectTx.Delete(subject).Error
			if err != nil {
				return fmt.Errorf("error deleting subject: %w", err)
			}

			return nil
		})

		if err != nil {
			render.Status(request, errorCode)
			if errorResponse == nil {
				errorResponse = map[string]interface{}{
					"error_code": 50001,
					"message":    fmt.Sprintf("error saving subject schema version: %s", err),
				}
			}

			render.JSON(writer, request, errorResponse)
			return
		}

		subjectVersionIDs := make([]int32, len(subjectVersions))
		for index, subjectVersion := range subjectVersions {
			subjectVersionIDs[index] = subjectVersion.Version
		}

		render.Status(request, http.StatusOK)
		render.JSON(writer, request, subjectVersionIDs)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)
	chiRouter.Get("/{subject}/versions/{version}", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		response := &ResponseGetSubjectVersion{}

		errorCode := http.StatusInternalServerError
		var errorResponse map[string]interface{}

		err := db.Transaction(func(tx *gorm.DB) error {
			subject := &dbModels.Subject{}
			err := tx.Clauses(forceIndexHint("idx_subjects_name")).
				Where("name = ?", subjectName).First(subject).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40401,
						"message":    "subject not found",
					}
					return fmt.Errorf("subject not found")
				}
				return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
			}

			getVersionTx := tx
			if version == "-1" || version == "latest" {
				getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_versions_subject_id")).Where("subject_id = ?", subject.ID).Order("version desc").Limit(1)
			} else {
				versionInt, err := strconv.ParseInt(version, 10, 32)
				if err != nil {
					errorCode = http.StatusUnprocessableEntity
					errorResponse = map[string]interface{}{
						"error_code": 42202,
						"message":    "invalid version",
					}
					return fmt.Errorf("invalid version")
				}
				getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_id_version")).Where("subject_id = ? AND VERSION = ?", subject.ID, versionInt)
			}

			versionModel := &dbModels.SubjectVersion{}
			err = getVersionTx.First(versionModel).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusUnprocessableEntity
					errorResponse = map[string]interface{}{
						"error_code": 40402,
						"message":    "version not found",
					}
					return fmt.Errorf("version not found")
				}
				return fmt.Errorf("error finding version %s for subject %s: %w", version, subjectName, err)
			}

			schema := &dbModels.Schema{}
			err = tx.Where("id = ?", versionModel.SchemaID).First(schema).Error
			if err != nil {
				return fmt.Errorf("error finding schema for version %s for subject %s: %w", version, subjectName, err)
			}

			response.Subject = subjectName
			response.ID = schema.SchemaID
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
			render.Status(request, errorCode)
			if errorResponse == nil {
				errorResponse = map[string]interface{}{
					"error_code": 50001,
					"message":    fmt.Sprintf("error getting subject schema version: %s", err),
				}
			}

			render.JSON(writer, request, errorResponse)
			return
		}

		render.Status(request, http.StatusOK)
		render.Render(writer, request, response)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)-schema
	chiRouter.Get("/{subject}/versions/{version}/schema", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		var schemaData string

		errorCode := http.StatusInternalServerError
		var errorResponse map[string]interface{}

		err := db.Transaction(func(tx *gorm.DB) error {
			subject := &dbModels.Subject{}
			err := tx.Clauses(forceIndexHint("idx_subjects_name")).
				Where("name = ?", subjectName).First(subject).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40401,
						"message":    "subject not found",
					}
					return fmt.Errorf("subject not found")
				}
				return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
			}

			getVersionTx := tx
			if version == "-1" || version == "latest" {
				getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_versions_subject_id")).Where("subject_id = ?", subject.ID).Order("version desc").Limit(1)
			} else {
				versionInt, err := strconv.ParseInt(version, 10, 32)
				if err != nil {
					errorCode = http.StatusUnprocessableEntity
					errorResponse = map[string]interface{}{
						"error_code": 42202,
						"message":    "invalid version",
					}
					return fmt.Errorf("invalid version")
				}
				getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_id_version")).Where("subject_id = ? AND VERSION = ?", subject.ID, versionInt)
			}

			versionModel := &dbModels.SubjectVersion{}
			err = getVersionTx.First(versionModel).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40402,
						"message":    "version not found",
					}
					return fmt.Errorf("version not found")
				}
				return fmt.Errorf("error finding version %s for subject %s: %w", version, subjectName, err)
			}

			schema := &dbModels.Schema{}
			err = tx.Where("id = ?", versionModel.SchemaID).First(schema).Error
			if err != nil {
				return fmt.Errorf("error finding schema for version %s for subject %s: %w", version, subjectName, err)
			}

			schemaData = schema.Schema

			return nil
		})

		if err != nil {
			render.Status(request, errorCode)
			if errorResponse == nil {
				errorResponse = map[string]interface{}{
					"error_code": 50001,
					"message":    fmt.Sprintf("error getting subject schema version: %s", err),
				}
			}

			render.JSON(writer, request, errorResponse)
			return
		}

		render.Status(request, http.StatusOK)
		writer.Write([]byte(schemaData))
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
	chiRouter.Post("/{subject}/versions", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		data := &RequestPostSubjectVersion{}

		response := &ResponsePostSubjectVersion{}

		if err := render.Bind(request, data); err != nil {
			render.Status(request, http.StatusUnprocessableEntity)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": http.StatusUnprocessableEntity,
				"message":    fmt.Sprintf("error parsing body: %s", err),
			})
			return
		}

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
				render.Status(request, http.StatusBadRequest)
				render.JSON(writer, request, map[string]interface{}{
					"error_code": http.StatusBadRequest,
					"message":    fmt.Sprintf("unknown schema type: %s", data.SchemaType),
				})
				return
			}
		}

		parsedSchema, err := schemas.ParseSchema(data.Schema, schemaType)
		if err != nil {
			render.Status(request, http.StatusBadRequest)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": http.StatusBadRequest,
				"message":    fmt.Sprintf("error parsing schema: %s", err),
			})
			return
		}

		errorCode := http.StatusInternalServerError
		var errorResponse map[string]interface{}

		err = db.Transaction(func(tx *gorm.DB) error {

			subject := &dbModels.Subject{}
			// unscoped so we can get soft deleted subjects
			err := tx.Unscoped().Clauses(forceIndexHint("idx_subjects_name")).
				Where("name = ?", subjectName).First(subject).Error
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
				nextId, err := dbModels.NextSequenceID(db, dbModels.SequenceNameSchemaIDs)
				if err != nil {
					errorCode = http.StatusInternalServerError
					errorResponse = map[string]interface{}{
						"error_code": http.StatusInternalServerError,
						"message":    fmt.Sprintf("error generating next schema id: %s", err),
					}
					return fmt.Errorf("error generating next schema id: %w", err)
				}

				if nextId > math.MaxInt32 {
					errorCode = http.StatusInternalServerError
					errorResponse = map[string]interface{}{
						"error_code": http.StatusInternalServerError,
						"message":    "too many schemas registered next schema id is greater than int32",
					}
					return fmt.Errorf("too many schemas registered next schema id is greater than int32")
				}

				// create it
				schema = &dbModels.Schema{
					ID:         uuid.New(),
					SchemaID:   int32(nextId),
					Schema:     data.Schema,
					Hash:       data.calculatedHash,
					SchemaType: dbSchemaType,
				}
				if err := tx.Create(schema).Error; err != nil {
					return fmt.Errorf("error creating schema for subject: %s: %w", subjectName, err)
				}
			}
			response.ID = schema.SchemaID

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
				// checking compatibility
				if subject.Compatibility != dbModels.SubjectCompatibilityNone {

					existingSchemas := make([]dbModels.Schema, 0)
					query := tx.Model(&dbModels.Schema{}).Clauses(forceIndexHint("idx_subject_versions_subject_id")).
						Joins("JOIN subject_versions ON subject_versions.schema_id = schemas.id").
						Order("version desc").
						Where("subject_id = ?", subject.ID)

					// check if we are transitive
					if !strings.HasSuffix(string(subject.Compatibility), "_TRANSITIVE") {
						// not transitive so we only need the first one
						query = query.Limit(1)
					} else {
						// we are transitive, this is most likely a very expensive operation, so it's probably not a good idea to do
						// this query could return tons of rows and require tons of comparisons
					}

					err := query.Find(&existingSchemas).Error
					if err != nil {
						return fmt.Errorf("error finding existing schemas for compatibility checking: %w", err)
					}

					var existingParsedSchemas []schemas.ParsedSchema
					for _, existingSchemaModel := range existingSchemas {
						existingParsedSchema, err := schemas.ParseSchema(existingSchemaModel.Schema, schemaType)
						if err != nil {
							errorCode = http.StatusInternalServerError
							errorResponse = map[string]interface{}{
								"error_code": http.StatusInternalServerError,
								"message":    fmt.Sprintf("error parsing existing: %s", err),
							}
							return fmt.Errorf("error parsing existing: %w", err)
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
								errorCode = http.StatusInternalServerError
								errorResponse = map[string]interface{}{
									"error_code": http.StatusInternalServerError,
									"message":    fmt.Sprintf("error checking compatibility: %s", err),
								}
								return fmt.Errorf("error checking compatibility: %w", err)
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
								errorCode = http.StatusInternalServerError
								errorResponse = map[string]interface{}{
									"error_code": http.StatusInternalServerError,
									"message":    fmt.Sprintf("error checking compatibility: %s", err),
								}
								return fmt.Errorf("error checking compatibility: %w", err)
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
								errorCode = http.StatusInternalServerError
								errorResponse = map[string]interface{}{
									"error_code": http.StatusInternalServerError,
									"message":    fmt.Sprintf("error checking compatibility: %s", err),
								}
								return fmt.Errorf("error checking compatibility: %w", err)
							}

							if isBackwardsCompatible == false {
								compatible = false
								break
							}

							isBackwardsCompatible, err = existingParsedSchema.IsBackwardsCompatible(parsedSchema)
							if err != nil {
								errorCode = http.StatusInternalServerError
								errorResponse = map[string]interface{}{
									"error_code": http.StatusInternalServerError,
									"message":    fmt.Sprintf("error checking compatibility: %s", err),
								}
								return fmt.Errorf("error checking compatibility: %w", err)
							}

							if isBackwardsCompatible == false {
								compatible = false
								break
							}
						}
						break
					}

					if compatible == false {
						errorCode = http.StatusConflict
						errorResponse = map[string]interface{}{
							"error_code": http.StatusConflict,
							"message":    fmt.Sprintf("schema is incompatible with an earlier schema"),
						}
						return fmt.Errorf("schema is incompatible with an earlier schema")
					}

				}

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
						errorCode = http.StatusBadRequest
						errorResponse = map[string]interface{}{
							"error_code": http.StatusBadRequest,
							"message":    "cannot add version of a different schema type",
						}
						return fmt.Errorf("cannot add version of a different schema type")
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
			render.Status(request, errorCode)
			if errorResponse == nil {
				errorResponse = map[string]interface{}{
					"error_code": 50001,
					"message":    fmt.Sprintf("error saving subject schema version: %s", err),
				}
			}

			render.JSON(writer, request, errorResponse)
			return
		}

		render.Status(request, http.StatusOK)
		render.Render(writer, request, response)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)
	chiRouter.Post("/{subject}", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		data := &RequestPostSubject{}

		response := &ResponsePostSubject{}

		if err := render.Bind(request, data); err != nil {
			render.Status(request, http.StatusUnprocessableEntity)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": http.StatusUnprocessableEntity,
				"message":    fmt.Sprintf("error parsing body: %s", err),
			})
			return
		}

		schemaType := schemas.SchemaTypeAvro
		if len(data.SchemaType) > 0 {
			switch data.SchemaType {
			case schemas.SchemaTypeAvro:
				schemaType = schemas.SchemaTypeAvro
			// TODO: uncomment once these other types are supported
			// case SchemaTypeJSON:
			// 	schemaType = SchemaTypeJSON
			// case SchemaTypeProtobuf:
			// 	schemaType = SchemaTypeProtobuf
			default:
				render.Status(request, http.StatusBadRequest)
				render.JSON(writer, request, map[string]interface{}{
					"error_code": http.StatusBadRequest,
					"message":    fmt.Sprintf("unknown schema type: %s", data.SchemaType),
				})
				return
			}
		}

		errorCode := http.StatusInternalServerError
		var errorResponse map[string]interface{}

		err := db.Transaction(func(tx *gorm.DB) error {
			subject := &dbModels.Subject{}
			err := tx.Clauses(forceIndexHint("idx_subjects_name")).
				Where("name = ?", subjectName).First(subject).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40401,
						"message":    "subject not found",
					}
					return fmt.Errorf("subject not found")
				}
				return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
			}

			schema := &dbModels.Schema{}
			err = tx.Clauses(forceIndexHint("idx_schemas_hash")).
				Where("hash = ? AND schema_type", data.calculatedHash, schemaType).First(schema).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40403,
						"message":    "schema not found",
					}
					return fmt.Errorf("schema not found")
				}
				return fmt.Errorf("error finding schema for subject %s: %w", subjectName, err)
			}

			subjectVersion := &dbModels.SubjectVersion{}
			err = tx.Clauses(forceIndexHint("idx_subject_id_schema_id")).
				Where("subject_id = ? AND schema_id = ?", subject.ID, schema.ID).First(subjectVersion).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40403,
						"message":    "schema not found",
					}
					return fmt.Errorf("schema not found")
				}
				return fmt.Errorf("error finding subject version for subject %s: %w", subjectName, err)
			}

			response.Subject = subject.Name
			response.ID = schema.SchemaID
			response.Version = subjectVersion.Version
			response.Schema = schema.Schema

			return nil
		})

		if err != nil {
			render.Status(request, errorCode)
			if errorResponse == nil {
				errorResponse = map[string]interface{}{
					"error_code": 50001,
					"message":    fmt.Sprintf("error saving subject schema version: %s", err),
				}
			}

			render.JSON(writer, request, errorResponse)
			return
		}

		render.Status(request, http.StatusOK)
		render.Render(writer, request, response)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)
	chiRouter.Delete("/{subject}/versions/{version}", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		errorCode := http.StatusInternalServerError
		var errorResponse map[string]interface{}

		permanentRaw := request.URL.Query().Get("permanent")
		permanent, _ := strconv.ParseBool(permanentRaw)

		err := db.Transaction(func(tx *gorm.DB) error {
			subject := &dbModels.Subject{}
			err := tx.Clauses(forceIndexHint("idx_subjects_name")).
				Where("name = ?", subjectName).First(subject).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					errorCode = http.StatusNotFound
					errorResponse = map[string]interface{}{
						"error_code": 40401,
						"message":    "subject not found",
					}
					return fmt.Errorf("subject not found")
				}
				return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
			}

			getVersionTx := tx
			if version == "-1" || version == "latest" {
				getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_versions_subject_id")).Where("subject_id = ?", subject.ID).Order("version desc").Limit(1)
			} else {
				versionInt, err := strconv.ParseInt(version, 10, 32)
				if err != nil {
					errorCode = http.StatusUnprocessableEntity
					errorResponse = map[string]interface{}{
						"error_code": 42202,
						"message":    "invalid version",
					}
					return fmt.Errorf("invalid version")
				}
				getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_id_version")).Where("subject_id = ? AND VERSION = ?", subject.ID, versionInt)
			}

			// only hard delete if a specific version is given, not latest
			if permanent && version != "-1" && version != "latest" {
				getVersionTx = getVersionTx.Unscoped()
			}

			versionModel := &dbModels.SubjectVersion{}
			txResp := getVersionTx.Delete(versionModel)
			err = txResp.Error
			if err != nil {
				return fmt.Errorf("error finding version %s for subject %s: %w", version, subjectName, err)
			}

			if txResp.RowsAffected == 0 {
				errorCode = http.StatusNotFound
				errorResponse = map[string]interface{}{
					"error_code": 40402,
					"message":    "version not found",
				}
				return fmt.Errorf("version not found")
			}

			return nil
		})

		if err != nil {
			render.Status(request, errorCode)
			if errorResponse == nil {
				errorResponse = map[string]interface{}{
					"error_code": 50001,
					"message":    fmt.Sprintf("error getting subject schema version: %s", err),
				}
			}

			render.JSON(writer, request, errorResponse)
			return
		}

		render.Status(request, http.StatusOK)
		writer.Write([]byte(version))
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-versionId-%20version-referencedby
	chiRouter.Get("/{subject}/versions/{version}/referencedby", func(writer http.ResponseWriter, request *http.Request) {

	})

	return chiRouter
}
