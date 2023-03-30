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
	SchemaID   int32
	Schema     string
	Hash       string
	SchemaType SchemaType
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  gorm.DeletedAt
}
