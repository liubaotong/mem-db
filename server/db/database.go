package db

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type ColumnType int

const (
	TypeInt ColumnType = iota
	TypeString
)

type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

type Table struct {
	Name    string                   `json:"name"`
	Columns []Column                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
	mu      sync.RWMutex            `json:"-"`
}

type Database struct {
	tables map[string]*Table
	mu     sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		tables: make(map[string]*Table),
	}
}

func (db *Database) CreateTable(name string, columns []Column) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	db.tables[name] = &Table{
		Name:    name,
		Columns: columns,
		Rows:    make([]map[string]interface{}, 0),
	}
	return nil
}

func (db *Database) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	return table, nil
}

func (db *Database) SaveToDisk(filename string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	data := make(map[string]TableData)
	for name, table := range db.tables {
		data[name] = TableData{
			Name:    table.Name,
			Columns: table.Columns,
			Rows:    table.Rows,
		}
	}

	encoder := json.NewEncoder(file)
	return encoder.Encode(data)
}

func (db *Database) LoadFromDisk(filename string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var data map[string]TableData
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return err
	}

	db.tables = make(map[string]*Table)

	for _, tableData := range data {
		db.tables[tableData.Name] = &Table{
			Name:    tableData.Name,
			Columns: tableData.Columns,
			Rows:    tableData.Rows,
		}
	}

	return nil
}

func (db *Database) GetTableInfo(name string) (*TableInfo, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", name)
	}

	columns := make([]struct {
		Name string
		Type string
	}, len(table.Columns))

	for i, col := range table.Columns {
		typeName := "string"
		if col.Type == TypeInt {
			typeName = "int"
		}
		columns[i] = struct {
			Name string
			Type string
		}{
			Name: col.Name,
			Type: typeName,
		}
	}

	return &TableInfo{
		Name:    table.Name,
		Columns: columns,
	}, nil
}

type TableData struct {
	Name    string                   `json:"name"`
	Columns []Column                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
}

type TableInfo struct {
	Name    string
	Columns []struct {
		Name string
		Type string
	}
} 