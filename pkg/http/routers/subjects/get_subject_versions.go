package subjects

import (
	"fmt"
	"net/http"

	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"gorm.io/gorm"
)

func getSubjectVersions(db *gorm.DB, subjectName string, includeDeleted bool) (*ResponseGetSubjectVersions, error) {

	var subjectVersions []dbModels.SubjectVersion

	subjectVersionsDB := db
	if includeDeleted {
		subjectVersionsDB = subjectVersionsDB.Unscoped()
	}
	err := subjectVersionsDB.Model(&dbModels.SubjectVersion{}).
		Clauses(forceIndexHint("idx_subjects_name")).
		Joins("JOIN subjects ON subjects.id = subject_versions.subject_id").
		Where("subjects.name = ? AND subjects.deleted_at is NULL", subjectName).
		Order("subject_versions.version asc").Find(&subjectVersions).Error

	if err != nil {
		return nil, err
	}

	if len(subjectVersions) == 0 {
		// TODO: no versions, so we should check if the subject actually exists, if it does return a empty list

		return nil, routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("subject not found"))
	}

	subjectVersionIDs := make(ResponseGetSubjectVersions, len(subjectVersions))
	for index, subjectVersion := range subjectVersions {
		subjectVersionIDs[index] = subjectVersion.Version
	}

	return &subjectVersionIDs, nil
}
