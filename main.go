package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-logr/zapr"
	"github.com/rmb938/franz-schema-registry/pkg/database/migrations"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers/schemas"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers/subjects"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	zc := zap.NewDevelopmentConfig()
	// zc := zap.NewProductionConfig()
	z, err := zc.Build()
	if err != nil {
		panic(fmt.Sprintf("who watches the watchmen (%v)?", err))
	}
	log := zapr.NewLogger(z)

	db, err := gorm.Open(postgres.Open(
		"host=localhost user=postgres password=postgres dbname=franz-schema-registry port=5432 sslmode=disable",
	), &gorm.Config{})
	if err != nil {
		log.Error(err, "error opening database connection")
		os.Exit(1)
	}

	log.Info("Running database migrations")
	if err = migrations.RunMigrations(db); err != nil {
		log.Error(err, "error running database migrations")
		os.Exit(1)
	}
	log.Info("Done running database migrations")

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger) // TODO: convert to logr
	r.Use(middleware.Recoverer)

	// Set a timeout value on the request context (ctx), that will signal
	// through ctx.Done() that the request has timed out and further
	// processing should be stopped.
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(middleware.Heartbeat("/ping"))

	r.Use(middleware.AllowContentType("application/json"))

	r.Mount("/schemas", schemas.NewRouter())
	r.Mount("/subjects", subjects.NewRouter(db))

	http.ListenAndServe(":8081", r)
}
