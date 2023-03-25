package schemas

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func NewRouter() *chi.Mux {
	chiRouter := chi.NewRouter()

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-types-
	chiRouter.Get("/types", func(writer http.ResponseWriter, request *http.Request) {

	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id
	chiRouter.Get("/ids/{id}", func(writer http.ResponseWriter, request *http.Request) {

	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id-schema
	chiRouter.Get("/ids/{id}/schema", func(writer http.ResponseWriter, request *http.Request) {

	})

	// https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id-versions
	chiRouter.Get("/ids/{id}/versions", func(writer http.ResponseWriter, request *http.Request) {

	})

	return chiRouter
}
