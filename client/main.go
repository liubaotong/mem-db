package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"bufio"
	"os"
	"strings"
	"strconv"
	"time"
	"io"
	"sort"
	"../server/protocol"  // 引入 protocol 包
)

// 添加客户端配置
const (
	SERVER_ADDR = "localhost:8080"
	MAX_RETRIES = 3
)

type Client struct {
	conn     net.Conn
	encoder  *json.Encoder
	decoder  *json.Decoder
	scanner  *bufio.Scanner
}

func NewClient() (*Client, error) {
	// 尝试连接服务器
	var conn net.Conn
	var err error
	for i := 0; i < MAX_RETRIES; i++ {
		conn, err = net.Dial("tcp", SERVER_ADDR)
		if err == nil {
			break
		}
		log.Printf("连接失败，重试 %d/%d: %v", i+1, MAX_RETRIES, err)
		if i < MAX_RETRIES-1 {
			time.Sleep(time.Second)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("无法连接到服务器: %v", err)
	}

	return &Client{
		conn:     conn,
		encoder:  json.NewEncoder(conn),
		decoder:  json.NewDecoder(conn),
		scanner:  bufio.NewScanner(os.Stdin),
	}, nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) Run() {
	fmt.Println("连接到服务器成功。输入 HELP 查看支持的命令。")
	
	for {
		fmt.Print("> ")
		if !c.scanner.Scan() {
			break
		}

		input := c.scanner.Text()
		if strings.TrimSpace(input) == "" {
			continue
		}

		if strings.ToUpper(input) == "HELP" {
			printHelp()
			continue
		}

		if strings.ToUpper(input) == "EXIT" {
			fmt.Println("再见！")
			break
		}

		if err := c.handleCommand(input); err != nil {
			fmt.Printf("错误: %v\n", err)
			if err == io.EOF {
				fmt.Println("与服务器的连接已断开")
				break
			}
		}
	}
}

func (c *Client) handleCommand(input string) error {
	cmd := parseCommand(input)
	if cmd.Type == -1 {
		return fmt.Errorf("无效的命令。输入 HELP 查看支持的命令格式")
	}

	// 发送命令到服务器
	if err := c.encoder.Encode(cmd); err != nil {
		return fmt.Errorf("发送命令失败: %v", err)
	}

	// 接收服务器响应
	var response protocol.Response
	if err := c.decoder.Decode(&response); err != nil {
		return err
	}

	// 处理响应
	if !response.Success {
		return fmt.Errorf("服务器错误: %s", response.Error)
	}

	// 根据命令类型格式化输出
	switch cmd.Type {
	case protocol.Select:
		c.displaySelectResult(response.Data)
	case protocol.Delete:
		fmt.Println(response.Data)
	case protocol.SaveToDisk:
		fmt.Println("数据库已保存")
	default:
		if response.Data != nil {
			fmt.Printf("成功: %v\n", response.Data)
		} else {
			fmt.Println("操作成功")
		}
	}

	return nil
}

// 格式化显示查询结果
func (c *Client) displaySelectResult(data interface{}) {
	rows, ok := data.([]interface{})
	if !ok || len(rows) == 0 {
		fmt.Println("没有找到记录")
		return
	}

	// 获取所有列名
	firstRow, ok := rows[0].(map[string]interface{})
	if !ok {
		fmt.Println("数据格式错误")
		return
	}

	columns := make([]string, 0, len(firstRow))
	for col := range firstRow {
		columns = append(columns, col)
	}
	sort.Strings(columns)  // 保证列顺序一致

	// 计算每列的最大宽度
	widths := make(map[string]int)
	for _, col := range columns {
		widths[col] = len(col)
	}

	for _, row := range rows {
		rowMap, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		for col, val := range rowMap {
			width := len(fmt.Sprintf("%v", val))
			if width > widths[col] {
				widths[col] = width
			}
		}
	}

	// 打印表头
	fmt.Println(strings.Repeat("-", calculateTableWidth(columns, widths)))
	for _, col := range columns {
		fmt.Printf("| %-*s ", widths[col], col)
	}
	fmt.Println("|")
	fmt.Println(strings.Repeat("-", calculateTableWidth(columns, widths)))

	// 打印数据行
	for _, row := range rows {
		rowMap, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		for _, col := range columns {
			fmt.Printf("| %-*v ", widths[col], rowMap[col])
		}
		fmt.Println("|")
	}
	fmt.Println(strings.Repeat("-", calculateTableWidth(columns, widths)))
	fmt.Printf("共 %d 条记录\n", len(rows))
}

func calculateTableWidth(columns []string, widths map[string]int) int {
	width := 1 // 开始的 |
	for _, col := range columns {
		width += widths[col] + 3 // 列宽 + " | "
	}
	return width
}

// 解析命令字符串为 Command 对象
func parseCommand(input string) protocol.Command {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return protocol.Command{Type: -1}
	}

	switch strings.ToUpper(parts[0]) {
	case "CREATE":
		return parseCreateTable(parts[1:])
	case "INSERT":
		return parseInsert(parts[1:])
	case "SELECT":
		return parseSelect(parts[1:])
	case "UPDATE":
		return parseUpdate(parts[1:])
	case "DELETE":
		return parseDelete(parts[1:])
	case "SAVE":
		return protocol.Command{Type: protocol.SaveToDisk}
	default:
		return protocol.Command{Type: -1}
	}
}

// 解析 CREATE TABLE 命令
func parseCreateTable(args []string) protocol.Command {
	// CREATE TABLE tablename (column1 type1, column2 type2)
	if len(args) < 4 || strings.ToUpper(args[0]) != "TABLE" {
		return protocol.Command{Type: -1}
	}

	tableName := args[1]
	columnsStr := strings.Join(args[2:], " ")
	
	// 提取括号中的内容
	start := strings.Index(columnsStr, "(")
	end := strings.LastIndex(columnsStr, ")")
	if start == -1 || end == -1 || start >= end {
		return protocol.Command{Type: -1}
	}

	// 解析列定义
	columnDefs := strings.Split(columnsStr[start+1:end], ",")
	columns := make([]struct{Name string; Type string}, 0)
	
	for _, def := range columnDefs {
		parts := strings.Fields(strings.TrimSpace(def))
		if len(parts) != 2 {
			return protocol.Command{Type: -1}
		}
		
		colType := strings.ToLower(parts[1])
		if colType != "int" && colType != "string" {
			return protocol.Command{Type: -1}
		}
		
		columns = append(columns, struct{Name string; Type string}{
			Name: parts[0],
			Type: colType,
		})
	}

	return protocol.Command{
		Type: protocol.CreateTable,
		Payload: protocol.CreateTablePayload{
			TableName: tableName,
			Columns:   columns,
		},
	}
}

// 解析 INSERT 命令
func parseInsert(args []string) protocol.Command {
	// INSERT INTO tablename VALUES (value1, value2, ...)
	if len(args) < 4 || strings.ToUpper(args[0]) != "INTO" || strings.ToUpper(args[2]) != "VALUES" {
		return protocol.Command{Type: -1}
	}

	tableName := args[1]
	valuesStr := strings.Join(args[3:], " ")
	
	// 提取括号中的内容
	start := strings.Index(valuesStr, "(")
	end := strings.LastIndex(valuesStr, ")")
	if start == -1 || end == -1 || start >= end {
		return protocol.Command{Type: -1}
	}

	// 解析值
	valueStrs := strings.Split(valuesStr[start+1:end], ",")
	values := make(map[string]interface{})
	
	for i, val := range valueStrs {
		val = strings.TrimSpace(val)
		// 尝试解析为整数
		if intVal, err := strconv.Atoi(val); err == nil {
			values[fmt.Sprintf("col%d", i+1)] = intVal
			continue
		}
		// 如果不是整数，去掉引号作为字符串处理
		values[fmt.Sprintf("col%d", i+1)] = strings.Trim(val, "\"'")
	}

	return protocol.Command{
		Type: protocol.Insert,
		Payload: protocol.InsertPayload{
			TableName: tableName,
			Values:    values,
		},
	}
}

// 解析 SELECT 命令
func parseSelect(args []string) protocol.Command {
	// SELECT * FROM tablename [WHERE condition1=value1 AND condition2=value2]
	if len(args) < 3 || args[0] != "*" || strings.ToUpper(args[1]) != "FROM" {
		return protocol.Command{Type: -1}
	}

	tableName := args[2]
	conditions := make(map[string]interface{})

	if len(args) > 3 {
		if strings.ToUpper(args[3]) == "WHERE" {
			whereConditions := args[4:]
			for i := 0; i < len(whereConditions); i++ {
				if strings.ToUpper(whereConditions[i]) == "AND" {
					continue
				}
				parts := strings.Split(whereConditions[i], "=")
				if len(parts) != 2 {
					return protocol.Command{Type: -1}
				}
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				
				// 尝试解析为整数
				if intVal, err := strconv.Atoi(value); err == nil {
					conditions[key] = intVal
				} else {
					// 如果不是整数，去掉引号作为字符串处理
					conditions[key] = strings.Trim(value, "\"'")
				}
			}
		}
	}

	return protocol.Command{
		Type: protocol.Select,
		Payload: protocol.SelectPayload{
			TableName:  tableName,
			Conditions: conditions,
		},
	}
}

// 解析 UPDATE 命令
func parseUpdate(args []string) protocol.Command {
	// UPDATE tablename SET column1=value1 [, column2=value2] [WHERE condition1=value1 AND condition2=value2]
	if len(args) < 4 || strings.ToUpper(args[2]) != "SET" {
		return protocol.Command{Type: -1}
	}

	tableName := args[1]
	values := make(map[string]interface{})
	conditions := make(map[string]interface{})

	// 找到 WHERE 子句的位置
	whereIndex := -1
	for i, arg := range args {
		if strings.ToUpper(arg) == "WHERE" {
			whereIndex = i
			break
		}
	}

	// 解析 SET 子句
	setArgs := args[3:]
	if whereIndex != -1 {
		setArgs = args[3:whereIndex]
	}

	// 处理 SET 子句中的赋值
	for _, arg := range setArgs {
		if strings.ToUpper(arg) == "," {
			continue
		}
		parts := strings.Split(arg, "=")
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// 尝试解析为整数
		if intVal, err := strconv.Atoi(value); err == nil {
			values[key] = intVal
		} else {
			// 如果不是整数，去掉引号作为字符串处理
			values[key] = strings.Trim(value, "\"'")
		}
	}

	// 解析 WHERE 子句
	if whereIndex != -1 {
		whereConditions := args[whereIndex+1:]
		for i := 0; i < len(whereConditions); i++ {
			if strings.ToUpper(whereConditions[i]) == "AND" {
				continue
			}
			parts := strings.Split(whereConditions[i], "=")
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			// 尝试解析为整数
			if intVal, err := strconv.Atoi(value); err == nil {
				conditions[key] = intVal
			} else {
				// 如果不是整数，去掉引号作为字符串处理
				conditions[key] = strings.Trim(value, "\"'")
			}
		}
	}

	return protocol.Command{
		Type: protocol.Update,
		Payload: protocol.UpdatePayload{
			TableName:  tableName,
			Values:    values,
			Conditions: conditions,
		},
	}
}

// 解析 DELETE 命令
func parseDelete(args []string) protocol.Command {
	// DELETE FROM tablename [WHERE condition1=value1 AND condition2=value2]
	if len(args) < 2 || strings.ToUpper(args[0]) != "FROM" {
		return protocol.Command{Type: -1}
	}

	tableName := args[1]
	conditions := make(map[string]interface{})

	// 解析 WHERE 子句
	whereIndex := -1
	for i, arg := range args {
		if strings.ToUpper(arg) == "WHERE" {
			whereIndex = i
			break
		}
	}

	if whereIndex != -1 {
		whereConditions := args[whereIndex+1:]
		for i := 0; i < len(whereConditions); i++ {
			if strings.ToUpper(whereConditions[i]) == "AND" {
				continue
			}
			parts := strings.Split(whereConditions[i], "=")
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			// 尝试解析为整数
			if intVal, err := strconv.Atoi(value); err == nil {
				conditions[key] = intVal
			} else {
				// 如果不是整数，去掉引号作为字符串处理
				conditions[key] = strings.Trim(value, "\"'")
			}
		}
	}

	return protocol.Command{
		Type: protocol.Delete,
		Payload: protocol.DeletePayload{
			TableName:  tableName,
			Conditions: conditions,
		},
	}
}

// 添加一个辅助函数来解析值
func parseValue(value string) interface{} {
	// 去掉首尾的空白字符
	value = strings.TrimSpace(value)
	
	// 尝试解析为整数
	if intVal, err := strconv.Atoi(value); err == nil {
		return intVal
	}
	
	// 如果不是整数，去掉引号作为字符串处理
	return strings.Trim(value, "\"'")
}

// 添加一个辅助函数来解析条件
func parseConditions(args []string) map[string]interface{} {
	conditions := make(map[string]interface{})
	for i := 0; i < len(args); i++ {
		if strings.ToUpper(args[i]) == "AND" {
			continue
		}
		parts := strings.Split(args[i], "=")
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		conditions[key] = parseValue(parts[1])
	}
	return conditions
}

// 打印帮助信息
func printHelp() {
	fmt.Println("\n支持的命令格式：")
	fmt.Println("1. CREATE TABLE tablename (column1 type1, column2 type2, ...)")
	fmt.Println("   支持的类型：int, string")
	fmt.Println("2. INSERT INTO tablename VALUES (value1, value2, ...)")
	fmt.Println("3. SELECT * FROM tablename [WHERE condition1=value1 AND condition2=value2]")
	fmt.Println("4. UPDATE tablename SET column1=value1 [, column2=value2] [WHERE condition1=value1]")
	fmt.Println("5. DELETE FROM tablename [WHERE condition1=value1]")
	fmt.Println("6. SAVE")
	fmt.Println("7. EXIT")
	fmt.Println("\n示例：")
	fmt.Println("CREATE TABLE users (id int, name string, age int)")
	fmt.Println("INSERT INTO users VALUES (1, \"Alice\", 20)")
	fmt.Println("SELECT * FROM users WHERE age=20")
	fmt.Println("UPDATE users SET age=21 WHERE name=\"Alice\"")
	fmt.Println("DELETE FROM users WHERE id=1")
	fmt.Println("SAVE")
	fmt.Println("")
}

func main() {
	client, err := NewClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.Run()
} 