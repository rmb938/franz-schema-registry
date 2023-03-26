package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Schema struct {
	gorm.Model
	ID        uuid.UUID
	SchemaID  int
	Schema    string
	Hash      string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}
