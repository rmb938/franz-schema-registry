package migrations

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

func RunMigrations(db *gorm.DB) error {

	migrations := make([]*gormigrate.Migration, 0)
	migrations = append(migrations, migration20230325130Init())

	return gormigrate.New(db, gormigrate.DefaultOptions, migrations).Migrate()
}
