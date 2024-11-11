package db

import (
	"fmt"
	"reflect"
)

// Insert 插入一行数据
func (t *Table) Insert(values map[string]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 创建一个新的行，确保所有列都有值
	row := make(map[string]interface{})
	
	// 验证并设置每个列的值
	for _, col := range t.Columns {
		val, ok := values[col.Name]
		if !ok {
			return fmt.Errorf("missing value for column %s", col.Name)
		}

		// 验证值类型
		if err := validateValueType(col, val); err != nil {
			return fmt.Errorf("column %s: %v", col.Name, err)
		}

		row[col.Name] = val
	}

	t.Rows = append(t.Rows, row)
	return nil
}

// Select 查询数据
func (t *Table) Select(condition func(map[string]interface{}) bool) []map[string]interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]map[string]interface{}, 0)
	for _, row := range t.Rows {
		if condition == nil || condition(row) {
			// 创建行的副本
			rowCopy := make(map[string]interface{})
			for k, v := range row {
				rowCopy[k] = v
			}
			result = append(result, rowCopy)
		}
	}
	return result
}

// Update 更新数据
func (t *Table) Update(condition func(map[string]interface{}) bool, values map[string]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 首先验证所有要更新的值的类型
	for _, col := range t.Columns {
		if val, ok := values[col.Name]; ok {
			if err := validateValueType(col, val); err != nil {
				return fmt.Errorf("column %s: %v", col.Name, err)
			}
		}
	}

	// 执行更新
	updated := false
	for i, row := range t.Rows {
		if condition == nil || condition(row) {
			updated = true
			// 只更新指定的列
			for colName, val := range values {
				row[colName] = val
			}
			t.Rows[i] = row
		}
	}

	if !updated {
		return fmt.Errorf("no matching records found")
	}
	return nil
}

// Delete 删除数据
func (t *Table) Delete(condition func(map[string]interface{}) bool) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	originalLen := len(t.Rows)
	newRows := make([]map[string]interface{}, 0, originalLen)
	
	for _, row := range t.Rows {
		if condition == nil || !condition(row) {
			newRows = append(newRows, row)
		}
	}

	deletedCount := originalLen - len(newRows)
	if deletedCount == 0 {
		return 0, fmt.Errorf("no matching records found")
	}

	t.Rows = newRows
	return deletedCount, nil
}

// 辅助函数：验证值类型
func validateValueType(col Column, val interface{}) error {
	switch col.Type {
	case TypeInt:
		switch v := val.(type) {
		case int:
			return nil
		case float64:
			// JSON 解码可能会将整数解析为 float64
			if v == float64(int(v)) {
				return nil
			}
			return fmt.Errorf("expected integer value, got float")
		default:
			return fmt.Errorf("expected int, got %v", reflect.TypeOf(val))
		}
	
	case TypeString:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string, got %v", reflect.TypeOf(val))
		}
		return nil
	
	default:
		return fmt.Errorf("unsupported column type: %v", col.Type)
	}
}

// GetColumns 返回表的列定义
func (t *Table) GetColumns() []Column {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	// 返回列定义的副本
	columns := make([]Column, len(t.Columns))
	copy(columns, t.Columns)
	return columns
}

// RowCount 返回表中的行数
func (t *Table) RowCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Rows)
} 