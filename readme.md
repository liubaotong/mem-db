# 开发一个简单的内存数据库

## 要求
- 用Golang开发
- 服务器功能
    - 工作目录为server
    - 表的创建、修改、删除，表类型就两种：int、string
    - 简单的SQL语句实现，可以操作表，只要基本的增、删、改、查功能
    - 实现一个tcp服务器，以供客户端链接，请求操作，设计一个简单的协议格式
    - 数据保存在内存中，并可以通过客户端命令同步到磁盘

- 客户端功能
    - 工作目录为client
    - 可以链接到服务器进行基本操作