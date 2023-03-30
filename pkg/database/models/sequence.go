package models

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
)

type SequenceName string

const (
	SequenceNameSchemaIDs SequenceName = "SCHEMA_IDS"
)

type Sequence struct {
	Name      SequenceName `gorm:"primarykey"`
	NextValue int64
}

func NextSequenceID(db *gorm.DB, name SequenceName) (int64, error) {
	nextValue := int64(0)

	err := db.Transaction(func(tx *gorm.DB) error {

		sequence := &Sequence{}
		err := tx.Where("name = ?", name).First(sequence).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) == false {
				return err
			}
			sequence = nil
		}

		if sequence == nil {
			sequence = &Sequence{}
			sequence.Name = name
		}

		sequence.NextValue += 1
		if err := tx.Save(sequence).Error; err != nil {
			return fmt.Errorf("error saving sequence: %w", err)
		}

		nextValue = sequence.NextValue
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("error running sequence transaction: %w", err)
	}

	return nextValue, nil
}
