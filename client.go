package mysql

import (
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/iooojik/go-logger"
	"time"
)

type Config struct {
	Dsn string
}

type Client struct {
	EnableLogs bool
	Connection *sql.DB
}

// NewClient создание клиента mysql
func NewClient(cfg Config) (*Client, error) {
	db, err := sql.Open("mysql", cfg.Dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(10 * time.Second)
	if err = db.Ping(); err != nil {
		return nil, err
	}
	logger.LogPositive("successfully connected to mysql")
	return &Client{
		Connection: db,
	}, nil
}

func (c *Client) GetStat() sql.DBStats {
	return c.Connection.Stats()
}

func (c *Client) Query(query string, args ...any) (*sql.Rows, error) {
	if c.EnableLogs {
		logger.LogDebug("executing", query, args)
	}
	return c.Connection.Query(query, args...)
}

func (c *Client) Execute(query string, args ...any) (sql.Result, error) {
	if c.EnableLogs {
		logger.LogDebug("executing", query, args)
	}
	return c.Connection.Exec(query, args...)
}

func ReadRows[T any](rows *sql.Rows) ([]T, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cols := make([]*string, len(colNames))
	colPtrs := make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}
	items := make([]T, 0)
	var myMap = make(map[string]any)
	for rows.Next() {
		scanErr := rows.Scan(colPtrs...)
		if scanErr != nil {
			return nil, scanErr
		}
		for i, col := range cols {
			myMap[colNames[i]] = col
		}
		rowItem := new(T)
		data, e := json.Marshal(myMap)
		if e != nil {
			return nil, e
		}
		e = json.Unmarshal(data, rowItem)
		if e != nil {
			return nil, e
		}
		items = append(items, *rowItem)
	}
	return items, nil
}
