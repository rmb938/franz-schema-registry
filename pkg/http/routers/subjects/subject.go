package subjects

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"gorm.io/gorm"
)

func getSubjectByName(tx *gorm.DB, subjectName string, includeDeleted bool) (*dbModels.Subject, error) {
	subject := &dbModels.Subject{}
	tx = tx.Clauses(forceIndexHint("idx_subjects_name")).Where("name = ?", subjectName)

	if includeDeleted {
		tx = tx.Unscoped()
	}

	err := tx.First(subject).Error
	if err != nil {
		return nil, err
	}

	return subject, nil
}

func getSubjectVersionBySubjectID(tx *gorm.DB, subjectID uuid.UUID, version string, includeDeleted bool) (*dbModels.SubjectVersion, error) {
	getVersionTx := tx
	if version == "-1" || version == "latest" {
		getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_versions_subject_id")).Where("subject_id = ?", subjectID).Order("version desc").Limit(1)
	} else {
		versionInt, err := strconv.ParseInt(version, 10, 32)
		if err != nil {
			return nil, routers.NewAPIError(http.StatusUnprocessableEntity, 42202, fmt.Errorf("invalid version"))
		}
		getVersionTx = getVersionTx.Clauses(forceIndexHint("idx_subject_id_version")).Where("subject_id = ? AND VERSION = ?", subjectID, versionInt)
	}

	if includeDeleted {
		getVersionTx = getVersionTx.Unscoped()
	}

	versionModel := &dbModels.SubjectVersion{}
	err := getVersionTx.First(versionModel).Error
	if err != nil {
		return nil, err
	}

	return versionModel, nil
}
