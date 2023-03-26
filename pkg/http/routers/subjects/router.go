package subjects

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/google/uuid"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"gorm.io/gorm"
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
			render.Status(request, http.StatusNotFound)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": 40401,
				"message":    fmt.Sprintf("subject not found"),
			})
			return
		}

		subjectVersionIDs := make([]int, len(subjectVersions))
		for index, subjectVersion := range subjectVersions {
			subjectVersionIDs[index] = subjectVersion.Version
		}

		render.Status(request, http.StatusOK)
		render.JSON(writer, request, subjectVersionIDs)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)
	chiRouter.Delete("/{subject}", func(writer http.ResponseWriter, request *http.Request) {

	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)
	chiRouter.Get("/{subject}/versions/{version}", func(writer http.ResponseWriter, request *http.Request) {

	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)-schema
	chiRouter.Get("/{subject}/versions/{version}/schema", func(writer http.ResponseWriter, request *http.Request) {

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

		err := db.Transaction(func(tx *gorm.DB) error {

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
				err := tx.Model(subject).Update("deleted_at", nil).Error
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
				// calculate int32 id, java clients use int32, so we can't go higher
				hash32a := fnv.New32a()
				if _, err := hash32a.Write([]byte(data.Schema)); err != nil {
					return fmt.Errorf("error calculating id of schema: %w", err)
				}
				calculatedId := int(binary.BigEndian.Uint32(hash32a.Sum(nil)))

				// make sure our id won't collide with an existing id
				existingSchema := &dbModels.Schema{}
				err = tx.Clauses(forceIndexHint("idx_schemas_schema_id")).
					Where("schema_id = ?", calculatedId).First(existingSchema).Error
				if err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) == false {
						return fmt.Errorf("error finding schema for subject %s: %w", subjectName, err)
					}
					existingSchema = nil
				}

				// id collision, all we can do is error
				if existingSchema != nil {
					return fmt.Errorf("error while trying to maintain compatibility with Confluent Schema Registry, calculated schema id %d already exists with a different hash", existingSchema.SchemaID)
				}

				// create it
				schema = &dbModels.Schema{
					ID:       uuid.New(),
					SchemaID: calculatedId,
					Schema:   data.Schema,
					Hash:     data.calculatedHash,
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
				latestVersion := &dbModels.SubjectVersion{}
				latestVersionNum := 0
				// unscoped because we need to include soft deleted and skip that version if it's soft deleted
				err = tx.Unscoped().Clauses(forceIndexHint("idx_subject_versions_subject_id")).
					Order("version desc").Where("subject_id = ?", subject.ID).First(latestVersion).Error
				if err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) == false {
						return fmt.Errorf("error finding latest version for subject %s: %w", subjectName, err)
					}
				}
				latestVersionNum = latestVersion.Version + 1

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
			render.Status(request, http.StatusInternalServerError)
			render.JSON(writer, request, map[string]interface{}{
				"error_code": 50001,
				"message":    fmt.Sprintf("error saving subject schema version: %s", err),
			})
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
				Where("hash = ?", data.calculatedHash).First(schema).Error
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

	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-versionId-%20version-referencedby
	chiRouter.Get("/{subject}/versions/{version}/referencedby", func(writer http.ResponseWriter, request *http.Request) {

	})

	return chiRouter
}
