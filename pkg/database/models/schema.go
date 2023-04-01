package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeJSON     SchemaType = "JSON"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

type Schema struct {
	gorm.Model
	ID         uuid.UUID
	GlobalID   int32
	Schema     string
	Hash       string
	SchemaType SchemaType
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  gorm.DeletedAt
}

type SchemaReference struct {
	ID               uuid.UUID
	SchemaID         uuid.UUID
	SubjectVersionID uuid.UUID
	Name             string
	CreatedAt        time.Time
	UpdatedAt        time.Time

	Schema         Schema
	SubjectVersion SubjectVersion
}
