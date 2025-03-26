package do

import (
	"time"
)

type Overview struct {
	Id         int       `gorm:"column:id;primaryKey;autoIncrement"`
	Database   string    `gorm:"column:database"`
	Coll       string    `gorm:"column:collection"`
	IndexNum   string    `gorm:"column:index_num"`
	NotFound   int       `gorm:"column:not_found"`
	Wrong      int       `gorm:"column:wrong"`
	CheckTotal int64     `gorm:"column:check_total"`
	CreatedAt  time.Time `gorm:"column:created_at"`
}

func (Overview) TableName() string {
	return "overview"
}
