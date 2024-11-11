package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"github.com/liubaotong/mem-db/server/db"
	"github.com/liubaotong/mem-db/server/protocol"
)

const (
	DEFAULT_DB_FILE = "database.json"
)

func main() {
	database := db.NewDatabase()
	
	// 设置优雅关闭
	setupGracefulShutdown(database)
	
	// 尝试加载已存在的数据库文件
	if _, err := os.Stat(DEFAULT_DB_FILE); err == nil {
		log.Printf("Loading existing database from %s\n", DEFAULT_DB_FILE)
		if err := database.LoadFromDisk(DEFAULT_DB_FILE); err != nil {
			log.Printf("Error loading database: %v\n", err)
		}
	}
	
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	
	log.Println("Server started on :8080")
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		
		go handleConnection(conn, database)
	}
}

func handleConnection(conn net.Conn, database *db.Database) {
	defer conn.Close()
	
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", remoteAddr)
	
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	
	for {
		var cmd protocol.Command
		if err := decoder.Decode(&cmd); err != nil {
			log.Printf("Client %s disconnected: %v", remoteAddr, err)
			return
		}
		
		log.Printf("Received command type %d from %s", cmd.Type, remoteAddr)
		
		response := handleCommand(cmd, database)
		
		if err := encoder.Encode(response); err != nil {
			log.Printf("Error sending response to %s: %v", remoteAddr, err)
			return
		}
	}
}

func handleCommand(cmd protocol.Command, database *db.Database) protocol.Response {
	switch cmd.Type {
	case protocol.CreateTable:
		return handleCreateTable(cmd.Payload, database)
	case protocol.Insert:
		return handleInsert(cmd.Payload, database)
	case protocol.Select:
		return handleSelect(cmd.Payload, database)
	case protocol.Update:
		return handleUpdate(cmd.Payload, database)
	case protocol.Delete:
		return handleDelete(cmd.Payload, database)
	case protocol.SaveToDisk:
		return handleSaveToDisk(database)
	case protocol.LoadFromDisk:
		return handleLoadFromDisk(cmd.Payload, database)
	case protocol.GetTableInfo:
		return handleGetTableInfo(cmd.Payload, database)
	default:
		return protocol.Response{
			Success: false,
			Error:   "unknown command",
		}
	}
}

func handleCreateTable(payload interface{}, database *db.Database) protocol.Response {
	createPayload, ok := payload.(protocol.CreateTablePayload)
	if !ok {
		return protocol.Response{Success: false, Error: "invalid payload"}
	}

	columns := make([]db.Column, len(createPayload.Columns))
	for i, col := range createPayload.Columns {
		var colType db.ColumnType
		switch col.Type {
		case "int":
			colType = db.TypeInt
		case "string":
			colType = db.TypeString
		default:
			return protocol.Response{
				Success: false, 
				Error: "invalid column type: " + col.Type,
			}
		}
		columns[i] = db.Column{Name: col.Name, Type: colType}
	}

	err := database.CreateTable(createPayload.TableName, columns)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	// 自动保存
	autoSave(database)
	return protocol.Response{Success: true}
}

func handleLoadFromDisk(payload interface{}, database *db.Database) protocol.Response {
	var filename string
	if payload != nil {
		var ok bool
		filename, ok = payload.(string)
		if !ok {
			return protocol.Response{Success: false, Error: "invalid filename"}
		}
	} else {
		filename = DEFAULT_DB_FILE
	}

	if err := database.LoadFromDisk(filename); err != nil {
		return protocol.Response{
			Success: false,
			Error:   fmt.Sprintf("failed to load database: %v", err),
		}
	}

	return protocol.Response{Success: true}
}

func handleGetTableInfo(payload interface{}, database *db.Database) protocol.Response {
	tableInfoPayload, ok := payload.(protocol.GetTableInfoPayload)
	if !ok {
		return protocol.Response{Success: false, Error: "invalid payload"}
	}

	tableInfo, err := database.GetTableInfo(tableInfoPayload.TableName)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	return protocol.Response{
		Success: true,
		Data:    tableInfo,
	}
}

func handleSaveToDisk(database *db.Database) protocol.Response {
	backupFile := DEFAULT_DB_FILE + ".bak"
	
	// 如果存在旧的数据库文件，先创建备份
	if _, err := os.Stat(DEFAULT_DB_FILE); err == nil {
		if err := os.Rename(DEFAULT_DB_FILE, backupFile); err != nil {
			log.Printf("Warning: failed to create backup: %v", err)
		}
	}

	if err := database.SaveToDisk(DEFAULT_DB_FILE); err != nil {
		// 如果保存失败，尝试恢复备份
		if _, err := os.Stat(backupFile); err == nil {
			if err := os.Rename(backupFile, DEFAULT_DB_FILE); err != nil {
				log.Printf("Critical: failed to restore backup: %v", err)
			}
		}
		return protocol.Response{Success: false, Error: err.Error()}
	}

	// 保存成功后删除备份
	if err := os.Remove(backupFile); err != nil {
		log.Printf("Warning: failed to remove backup file: %v", err)
	}

	return protocol.Response{Success: true}
}

func handleDelete(payload interface{}, database *db.Database) protocol.Response {
	deletePayload, ok := payload.(protocol.DeletePayload)
	if !ok {
		return protocol.Response{Success: false, Error: "invalid payload"}
	}

	table, err := database.GetTable(deletePayload.TableName)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	condition := func(row map[string]interface{}) bool {
		for k, v := range deletePayload.Conditions {
			if row[k] != v {
				return false
			}
		}
		return true
	}

	count, err := table.Delete(condition)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	// 自动保存
	autoSave(database)
	return protocol.Response{
		Success: true,
		Data:    fmt.Sprintf("Deleted %d records", count),
	}
}

func autoSave(database *db.Database) {
	if err := database.SaveToDisk(DEFAULT_DB_FILE); err != nil {
		log.Printf("Warning: auto-save failed: %v", err)
	}
}

func handleInsert(payload interface{}, database *db.Database) protocol.Response {
	insertPayload, ok := payload.(protocol.InsertPayload)
	if !ok {
		return protocol.Response{Success: false, Error: "invalid payload"}
	}

	table, err := database.GetTable(insertPayload.TableName)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	err = table.Insert(insertPayload.Values)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	// 自动保存
	autoSave(database)
	return protocol.Response{Success: true}
}

func handleUpdate(payload interface{}, database *db.Database) protocol.Response {
	updatePayload, ok := payload.(protocol.UpdatePayload)
	if !ok {
		return protocol.Response{Success: false, Error: "invalid payload"}
	}

	table, err := database.GetTable(updatePayload.TableName)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	condition := func(row map[string]interface{}) bool {
		for k, v := range updatePayload.Conditions {
			if row[k] != v {
				return false
			}
		}
		return true
	}

	err = table.Update(condition, updatePayload.Values)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	// 自动保存
	autoSave(database)
	return protocol.Response{Success: true}
}

func handleSelect(payload interface{}, database *db.Database) protocol.Response {
	selectPayload, ok := payload.(protocol.SelectPayload)
	if !ok {
		return protocol.Response{Success: false, Error: "invalid payload"}
	}

	table, err := database.GetTable(selectPayload.TableName)
	if err != nil {
		return protocol.Response{Success: false, Error: err.Error()}
	}

	condition := func(row map[string]interface{}) bool {
		for k, v := range selectPayload.Conditions {
			if row[k] != v {
				return false
			}
		}
		return true
	}

	result := table.Select(condition)
	return protocol.Response{
		Success: true,
		Data:    result,
	}
}

func setupGracefulShutdown(database *db.Database) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down server...")
		if err := database.SaveToDisk(DEFAULT_DB_FILE); err != nil {
			log.Printf("Error saving database: %v", err)
		}
		os.Exit(0)
	}()
}