package subjects

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
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
		render.Status(request, http.StatusOK)
		// Whether to included soft deleted subjects
		deletedRaw := request.URL.Query().Get("deleted")
		deleted, _ := strconv.ParseBool(deletedRaw)

		var v render.Renderer
		v, err := getSubjects(db, deleted)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error listing subjects: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions
	chiRouter.Get("/{subject}/versions", func(writer http.ResponseWriter, request *http.Request) {
		render.Status(request, http.StatusOK)
		subjectName := chi.URLParam(request, "subject")

		// Whether to included soft deleted versions
		deletedRaw := request.URL.Query().Get("deleted")
		deleted, _ := strconv.ParseBool(deletedRaw)

		var v render.Renderer
		v, err := getSubjectVersions(db, subjectName, deleted)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error listing subject versions: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)
	chiRouter.Delete("/{subject}", func(writer http.ResponseWriter, request *http.Request) {
		render.Status(request, http.StatusOK)
		subjectName := chi.URLParam(request, "subject")

		permanentRaw := request.URL.Query().Get("permanent")
		permanent, _ := strconv.ParseBool(permanentRaw)

		var v render.Renderer
		v, err := deleteSubject(db, subjectName, permanent)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error deleting subject: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)
	chiRouter.Get("/{subject}/versions/{version}", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		var v render.Renderer
		v, err := getSubjectVersion(db, subjectName, version)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error getting subject version: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)-schema
	chiRouter.Get("/{subject}/versions/{version}/schema", func(writer http.ResponseWriter, request *http.Request) {
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		var v render.Renderer
		v, err := getSubjectVersionSchema(db, subjectName, version)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error getting subject version: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
	chiRouter.Post("/{subject}/versions", func(writer http.ResponseWriter, request *http.Request) {
		render.Status(request, http.StatusOK)
		subjectName := chi.URLParam(request, "subject")
		data := &RequestPostSubjectVersion{}

		var v render.Renderer

		if err := render.Bind(request, data); err != nil {
			v = routers.NewAPIError(http.StatusUnprocessableEntity, http.StatusUnprocessableEntity, fmt.Errorf("error parsing body: %w", err))
		}

		if v == nil {
			var err error
			v, err = postSubjectVersion(db, db, subjectName, data)
			if err != nil {
				v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error saving schema: %w", err))
				if renderer, ok := err.(render.Renderer); ok {
					v = renderer
				}
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)
	chiRouter.Post("/{subject}", func(writer http.ResponseWriter, request *http.Request) {
		render.Status(request, http.StatusOK)
		subjectName := chi.URLParam(request, "subject")
		data := &RequestPostSubject{}

		var v render.Renderer

		if err := render.Bind(request, data); err != nil {
			v = routers.NewAPIError(http.StatusUnprocessableEntity, http.StatusUnprocessableEntity, fmt.Errorf("error parsing body: %w", err))
		}

		if v == nil {
			var err error
			v, err = postSubject(db, subjectName, data)
			if err != nil {
				v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error checking schema: %w", err))
				if renderer, ok := err.(render.Renderer); ok {
					v = renderer
				}
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)
	chiRouter.Delete("/{subject}/versions/{version}", func(writer http.ResponseWriter, request *http.Request) {
		render.Status(request, http.StatusOK)
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		permanentRaw := request.URL.Query().Get("permanent")
		permanent, _ := strconv.ParseBool(permanentRaw)

		var v render.Renderer
		v, err := deleteSubjectVersion(db, subjectName, version, permanent)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error deleting subject version: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-versionId-%20version-referencedby
	chiRouter.Get("/{subject}/versions/{version}/referencedby", func(writer http.ResponseWriter, request *http.Request) {
		render.Status(request, http.StatusOK)
		subjectName := chi.URLParam(request, "subject")
		version := chi.URLParam(request, "version")

		var v render.Renderer
		v, err := getSubjectVersionReferencedBy(db, subjectName, version)
		if err != nil {
			v = routers.NewAPIError(http.StatusInternalServerError, 5001, fmt.Errorf("error getting subject version references: %w", err))
			if renderer, ok := err.(render.Renderer); ok {
				v = renderer
			}
		}

		render.Render(writer, request, v)
	})

	return chiRouter
}
