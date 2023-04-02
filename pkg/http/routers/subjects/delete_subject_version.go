package subjects

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/rmb938/franz-schema-registry/pkg/http/routers"
	"gorm.io/gorm"
)

func deleteSubjectVersion(db *gorm.DB, subjectName string, version string, permanent bool) (*ResponseDeleteSubjectVersion, error) {
	var resp ResponseDeleteSubjectVersion

	err := db.Transaction(func(tx *gorm.DB) error {
		subject, err := getSubjectByName(tx, subjectName)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("subject not found"))
			}
			return fmt.Errorf("error finding subject: %s: %w", subjectName, err)
		}

		if permanent && (version == "-1" || version == "latest") {
			return routers.NewAPIError(http.StatusBadRequest, 40001, fmt.Errorf("cannot permantly delete latest version"))
		}

		versionModel, err := getSubjectVersionBySubjectID(tx, subject.ID, version, permanent)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return routers.NewAPIError(http.StatusNotFound, 40401, fmt.Errorf("version not found"))
			}
			return fmt.Errorf("error finding version %s for subject %s: %w", version, subjectName, err)
		}

		if permanent && versionModel.DeletedAt.Valid == false {
			return routers.NewAPIError(http.StatusConflict, 40901, fmt.Errorf("must soft delete version %d first", versionModel.Version))
		}

		deleteTx := tx
		if permanent {
			deleteTx = deleteTx.Unscoped()
		}

		txResp := deleteTx.Delete(versionModel)
		err = txResp.Error
		if err != nil {
			return fmt.Errorf("error deleting version %s for subject %s: %w", version, subjectName, err)
		}

		resp = ResponseDeleteSubjectVersion(versionModel.Version)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &resp, nil
}
