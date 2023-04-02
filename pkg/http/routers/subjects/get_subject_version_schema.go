package subjects

import (
	"gorm.io/gorm"
)

func getSubjectVersionSchema(db *gorm.DB, subjectName string, version string) (*ResponseGetSubjectVersionSchema, error) {

	resp, err := getSubjectVersion(db, subjectName, version)
	if err != nil {
		return nil, err
	}

	schema := ResponseGetSubjectVersionSchema(resp.Schema)

	return &schema, nil
}
