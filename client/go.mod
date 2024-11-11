module github.com/liubaotong/mem-db/client

go 1.23.2

require (
	github.com/chzyer/readline v1.5.1
	github.com/liubaotong/mem-db/server v0.0.0
)

require golang.org/x/sys v0.0.0-20220310020820-b874c991c1a5 // indirect

replace github.com/liubaotong/mem-db/server => ../server
