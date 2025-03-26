package do

import "time"

type ResultFirst struct {
	Id        int       `gorm:"column:id;primaryKey;autoIncrement"`
	Database  string    `gorm:"column:database"`
	Coll      string    `gorm:"column:collection"`
	MID       string    `gorm:"column:mongod_pid"`
	SrcBson   string    `gorm:"column:src_bson"`
	DestBson  string    `gorm:"column:dest_bson"`
	WrongType string    `gorm:"column:wrong_type"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (ResultFirst) TableName() string {
	return "result_first"
}
