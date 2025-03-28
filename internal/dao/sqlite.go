package dao

import (
	"database/sql"
	"gorm.io/gorm"

	"mongo-checker/internal/model/do"
	"mongo-checker/internal/model/dto"
	gormv2 "mongo-checker/pkg/gorm"
	l "mongo-checker/pkg/log"
)

type SqliteDao struct {
	db *gormv2.Engine
}

func NewSqliteDao(path string) (*SqliteDao, error) {
	d, err := gormv2.NewSqlite(path)
	if err != nil {
		return nil, err
	}
	err = d.AutoMigrate(&do.Overview{}, &do.ResultRecord{})
	if err != nil {
		return nil, err
	}

	return &SqliteDao{db: d}, nil
}

func (d *SqliteDao) Connect() *gorm.DB {
	return d.db.Connect()
}

func (d *SqliteDao) Transaction(fn func(tx *gorm.DB) error) error {
	return d.db.Connect().Transaction(fn)
}

// Close the resource.
func (d *SqliteDao) Close() {
	conn, err := d.db.Connect().DB()
	if err != nil {
		l.Logger.Errorf("DB_Close_Err: [%+v]", err)
		return
	}
	if err = conn.Close(); err != nil {
		l.Logger.Errorf("DB_Close_Err: [%+v]", err)
	}
	l.Logger.Info("DB_Close_Success")
}

func (d *SqliteDao) CreateOverview(ov do.Overview) error {
	return d.db.Connect().Table(do.Overview{}.TableName()).Create(&ov).Error
}

func (d *SqliteDao) UpdateOverview(ns dto.NS, updates map[string]any) error {
	return d.db.Connect().Table(do.Overview{}.TableName()).
		Where("database = ? and collection = ?", ns.Database, ns.Collection).
		Updates(updates).Error
}

func (d *SqliteDao) GetNeedCheckNS() (ov []*do.Overview, err error) {
	err = d.db.Connect().Table(do.Overview{}.TableName()).
		Where("not_found > 0 or wrong > 0").
		Find(&ov).Error
	return
}

func (d *SqliteDao) CreateResultRecord(rr []*do.ResultRecord) error {
	return d.db.Connect().Table(do.ResultRecord{}.TableName()).Create(&rr).Error
}

func (d *SqliteDao) GetNSWrongRecord(db, col string) (rows *sql.Rows, cnt int64, err error) {
	rows, err = d.db.Connect().Table(do.ResultRecord{}.TableName()).
		Where("database = ? and collection = ? and seq_num = ?", db, col, dto.First).
		Count(&cnt).
		Rows()

	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return
	}
	return
}
