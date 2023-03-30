package migrations

import (
	"time"

	"github.com/go-gormigrate/gormigrate/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func migration20230325130Init() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "20230325130_init",
		Migrate: func(tx *gorm.DB) error {
			type Sequence struct {
				Name      string `gorm:"primaryKey"`
				NextValue int64
			}

			type Schema struct {
				gorm.Model
				ID         uuid.UUID `gorm:"primaryKey"`
				SchemaID   int32     `gorm:"uniqueIndex;not null"`
				Schema     string    `gorm:"not null"`
				Hash       string    `gorm:"uniqueIndex;not null"`
				SchemaType string    `gorm:"not null"`
				CreatedAt  time.Time `gorm:"not null"`
				UpdatedAt  time.Time `gorm:"not null"`
				DeletedAt  gorm.DeletedAt
			}

			type Subject struct {
				gorm.Model
				ID            uuid.UUID `gorm:"primaryKey"`
				Name          string    `gorm:"uniqueIndex;not null"`
				Compatibility string    `gorm:"not null"`
				CreatedAt     time.Time `gorm:"not null"`
				UpdatedAt     time.Time `gorm:"not null"`
				DeletedAt     gorm.DeletedAt
			}

			type SubjectVersion struct {
				gorm.Model
				ID        uuid.UUID `gorm:"primaryKey"`
				SubjectID uuid.UUID `gorm:"index:idx_subject_id_schema_id;index;uniqueIndex:idx_subject_id_version;not null"`
				SchemaID  uuid.UUID `gorm:"index:idx_subject_id_schema_id;not null"`
				Version   int32     `gorm:"uniqueIndex:idx_subject_id_version;not null"`
				CreatedAt time.Time `gorm:"not null"`
				UpdatedAt time.Time `gorm:"not null"`
				DeletedAt gorm.DeletedAt

				// Used to set up foreign keys, not used in actual model
				// Spanner does not support cascade, so we have to delete all versions manually when hard deleting
				Subject Subject
				Schema  Schema
			}

			return tx.Migrator().AutoMigrate(&Sequence{}, &Subject{}, &Schema{}, &SubjectVersion{})
		},
		Rollback: func(tx *gorm.DB) error {
			if err := tx.Migrator().DropTable("sequences"); err != nil {
				return err
			}
			if err := tx.Migrator().DropTable("subject_versions"); err != nil {
				return err
			}
			if err := tx.Migrator().DropTable("subjects"); err != nil {
				return err
			}
			if err := tx.Migrator().DropTable("schemas"); err != nil {
				return err
			}
			return nil
		},
	}
}
