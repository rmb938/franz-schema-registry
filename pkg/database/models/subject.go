package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type SubjectCompatibility string

const (
	SubjectCompatibilityBackward           SubjectCompatibility = "BACKWARD"
	SubjectCompatibilityBackwardTransitive SubjectCompatibility = "BACKWARD_TRANSITIVE"
	SubjectCompatibilityForward            SubjectCompatibility = "FORWARD"
	SubjectCompatibilityForwardTransitive  SubjectCompatibility = "FORWARD_TRANSITIVE"
	SubjectCompatibilityFull               SubjectCompatibility = "FULL"
	SubjectCompatibilityFullTransitive     SubjectCompatibility = "FULL_TRANSITIVE"
	SubjectCompatibilityNone               SubjectCompatibility = "NONE"
)

type Subject struct {
	gorm.Model
	ID            uuid.UUID
	Name          string
	Compatibility SubjectCompatibility
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DeletedAt     gorm.DeletedAt
}

type SubjectVersion struct {
	gorm.Model
	ID        uuid.UUID
	SubjectID uuid.UUID
	SchemaID  uuid.UUID
	Version   int
	CreatedAt time.Time
	DeletedAt gorm.DeletedAt
}
