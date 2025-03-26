package main

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
)

type Database struct {
    Host string
    Port int
}

func (r *Database) Get(id string) (string, error) {
    // Implements DB interface
}

// repo.go
type DB interface {
    Get(id string) (string, error)
}

type Repository struct {
    db DB
	getMoneoThings func(c *gin.Context)
	getMoneoThingByIdAndUnique func(c *gin.Context)
	getMoneoThingByValue func(c *gin.Context)
	getRawDataByValue func(c *gin.Context)
	getMoneoThingRawDataByTimeStamp func(c *gin.Context)
	getMoneoThingRawDataByTimeRange func(c *gin.Context)
	connectDB func() (driver.Conn, error)
	insertData func()
}

func NewRepository(db DB) *Repository {
    return &Repository{db: db}
}

func (r *Repository) Get(id string) (string, error) {
    return r.db.Get(id)
}