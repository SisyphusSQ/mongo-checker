package do

import (
	"time"
)

type Overview struct {
	Id         int       `gorm:"column:id;primaryKey;autoIncrement"`
	Database   string    `gorm:"column:database"`
	Coll       string    `gorm:"column:collection"`
	NotFound   int64     `gorm:"column:not_found"`
	Wrong      int64     `gorm:"column:wrong"`
	Total      int64     `gorm:"column:total"`
	CheckTotal int64     `gorm:"column:check_total"`
	CreatedAt  time.Time `gorm:"column:created_at"`
}

func (Overview) TableName() string {
	return "overview"
}
