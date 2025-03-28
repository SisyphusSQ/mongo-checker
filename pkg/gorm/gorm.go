package gormv2

import (
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"mongo-checker/pkg/log"
)

type Engine struct {
	gorm *gorm.DB
}

func (db *Engine) Connect() *gorm.DB {
	return db.gorm
}

func NewSqlite(path string) (*Engine, error) {
	db, err := gorm.Open(sqlite.Open(path+"/result.db"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	gormEngine := &Engine{db}
	gormEngine.wrapLog()
	return gormEngine, nil
}

func (db *Engine) AutoMigrate(tables ...any) error {
	for _, table := range tables {
		err := db.gorm.AutoMigrate(table)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Engine) wrapLog() {
	if log.Logger == nil {
		return
	}

	newLogger := logger.New(
		log.Logger,
		logger.Config{
			SlowThreshold:             200 * time.Millisecond, // Slow SQL threshold
			LogLevel:                  logger.Info,            // Log level
			IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,                  // Disable color
		},
	)
	db.gorm.Logger = newLogger
}

func (db *Engine) Close() error {
	d, err := db.gorm.DB()
	if err != nil {
		return err
	}

	err = d.Close()
	return err
}
