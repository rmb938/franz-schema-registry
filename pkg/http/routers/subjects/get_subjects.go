package subjects

import (
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"gorm.io/gorm"
)

func getSubjects(db *gorm.DB, includeDeleted bool) (*ResponseGetSubjects, error) {
	var subjects []dbModels.Subject

	subjectsDB := db
	if includeDeleted {
		subjectsDB = subjectsDB.Unscoped()
	}

	err := subjectsDB.Find(&subjects).Error
	if err != nil {
		return nil, err
	}

	subjectList := make(ResponseGetSubjects, len(subjects))
	for index, subject := range subjects {
		subjectList[index] = subject.Name
	}

	return &subjectList, nil
}
