package subjects

import (
	"errors"
	"fmt"
	"net/http"

	dbModels "github.com/rmb938/franz-schema-registry/pkg/database/models"
	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func deleteSubject(db *gorm.DB, subjectName string, permanent bool) (*ResponseDeleteSubjectVersions, error) {

	var subjectVersions []dbModels.SubjectVersion

	err := db.Transaction(func(tx *gorm.DB) error {
		subject := &dbModels.Subject{}
		err := tx.Unscoped().Clauses(forceIndexHint("idx_subjects_name")).
			Where("name = ?", subjectName).First(subject).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("subject not found"))
			}
			return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
		}

		if permanent && subject.DeletedAt.Valid == false {
			return routers.NewAPIError(http.StatusConflict, 40901, fmt.Errorf("must soft delete first"))
		}

		deleteVersionsTx := tx
		if permanent {
			deleteVersionsTx = deleteVersionsTx.Unscoped()
		}
		err = deleteVersionsTx.Clauses(clause.Returning{}).Where("subject_id = ?", subject.ID).Delete(&subjectVersions).Error
		if err != nil {
			return fmt.Errorf("error deleting subject versions: %w", err)
		}

		deleteSubjectTx := tx
		if permanent {
			deleteSubjectTx = deleteSubjectTx.Unscoped()
		}
		err = deleteSubjectTx.Delete(subject).Error
		if err != nil {
			return fmt.Errorf("error deleting subject versions: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	subjectVersionIDs := make(ResponseDeleteSubjectVersions, len(subjectVersions))
	for index, subjectVersion := range subjectVersions {
		subjectVersionIDs[index] = subjectVersion.Version
	}

	return &subjectVersionIDs, nil
}
