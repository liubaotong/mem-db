package protocol

import (
	"encoding/json"
	"fmt"
)

type CommandType int

const (
	CreateTable CommandType = iota
	Insert
	Select
	Update
	Delete
	SaveToDisk
	LoadFromDisk
	GetTableInfo
)

// String 方法用于将命令类型转换为字符串
func (ct CommandType) String() string {
	switch ct {
	case CreateTable:
		return "CREATE_TABLE"
	case Insert:
		return "INSERT"
	case Select:
		return "SELECT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case SaveToDisk:
		return "SAVE"
	case LoadFromDisk:
		return "LOAD"
	case GetTableInfo:
		return "GET_TABLE_INFO"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	Type    CommandType  `json:"type"`
	Payload interface{} `json:"payload"`
}

// UnmarshalJSON 自定义 JSON 解析
func (c *Command) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type    CommandType      `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	c.Type = raw.Type
	switch c.Type {
	case CreateTable:
		var payload CreateTablePayload
		if err := json.Unmarshal(raw.Payload, &payload); err != nil {
			return fmt.Errorf("invalid create table payload: %v", err)
		}
		c.Payload = payload
	case Insert:
		var payload InsertPayload
		if err := json.Unmarshal(raw.Payload, &payload); err != nil {
			return fmt.Errorf("invalid insert payload: %v", err)
		}
		c.Payload = payload
	case Select:
		var payload SelectPayload
		if err := json.Unmarshal(raw.Payload, &payload); err != nil {
			return fmt.Errorf("invalid select payload: %v", err)
		}
		c.Payload = payload
	case Update:
		var payload UpdatePayload
		if err := json.Unmarshal(raw.Payload, &payload); err != nil {
			return fmt.Errorf("invalid update payload: %v", err)
		}
		c.Payload = payload
	case Delete:
		var payload DeletePayload
		if err := json.Unmarshal(raw.Payload, &payload); err != nil {
			return fmt.Errorf("invalid delete payload: %v", err)
		}
		c.Payload = payload
	case GetTableInfo:
		var payload GetTableInfoPayload
		if err := json.Unmarshal(raw.Payload, &payload); err != nil {
			return fmt.Errorf("invalid get table info payload: %v", err)
		}
		c.Payload = payload
	}
	return nil
}

type CreateTablePayload struct {
	TableName string `json:"table_name"`
	Columns   []struct {
		Name string `json:"name"`
		Type string `json:"type"` // "int" or "string"
	} `json:"columns"`
}

type InsertPayload struct {
	TableName string                 `json:"table_name"`
	Values    map[string]interface{} `json:"values"`
}

type SelectPayload struct {
	TableName  string                 `json:"table_name"`
	Conditions map[string]interface{} `json:"conditions,omitempty"`
}

type UpdatePayload struct {
	TableName  string                 `json:"table_name"`
	Values     map[string]interface{} `json:"values"`
	Conditions map[string]interface{} `json:"conditions,omitempty"`
}

type DeletePayload struct {
	TableName  string                 `json:"table_name"`
	Conditions map[string]interface{} `json:"conditions,omitempty"`
}

type GetTableInfoPayload struct {
	TableName string `json:"table_name"`
}

type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// 用于序列化和反序列化的数据库结构
type DatabaseData struct {
	Tables map[string]TableData `json:"tables"`
	Version string             `json:"version"`  // 添加版本信息
	Created string             `json:"created"`  // 添加创建时间
	Updated string             `json:"updated"`  // 添加更新时间
}

type TableData struct {
	Name    string                   `json:"name"`
	Columns []ColumnData             `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
	Created string                   `json:"created"`  // 添加创建时间
	Updated string                   `json:"updated"`  // 添加更新时间
}

// ColumnType 定义列的数据类型
type ColumnType string

const (
	IntType    ColumnType = "int"
	StringType ColumnType = "string"
)

type ColumnData struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	Nullable bool       `json:"nullable,omitempty"`  // 添加可空标志
	Default  interface{} `json:"default,omitempty"`  // 添加默认值
}

// 添加错误类型
type ErrorCode int

const (
	ErrNone ErrorCode = iota
	ErrInvalidCommand
	ErrTableNotFound
	ErrColumnNotFound
	ErrInvalidType
	ErrDuplicateTable
	ErrDuplicateColumn
	ErrIOError
)

// Error 结构体用于标准化错误响应
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

func (e Error) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

// 创建标准错误的辅助函数
func NewError(code ErrorCode, message string) Error {
	return Error{
		Code:    code,
		Message: message,
	}
}