package go_toolbox

import "C"
import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"net/url"
	"time"
)

const (
	ClientMaxBlockSize        int = 100000
	ClientReadTimeout         int = 90
	ClientReadTimeoutDuration     = 90 * time.Second
)

type ClickhouseConf struct {
	Host       string   `json:"Host"`
	Port       int      `json:"Port"`
	Username   string   `json:"Username"`
	Password   string   `json:"PassWord"`
	Database   string   `json:"DataBase"`
	Table      string   `json:"Table"`
	DataSchema []string `json:"DataSchema"`
}
type CKHandler struct {
	ClickhouseConf
	clickHouseConnect *sqlx.DB
	InsertSql         string
}

func (c *CKHandler) QueryData(items interface{}, query string) error {
	//err := c.clickHouseConnect.Select(items, query)
	ctx, cancel := context.WithTimeout(context.Background(), ClientReadTimeoutDuration)
	defer cancel()
	err := c.clickHouseConnect.SelectContext(ctx, items, query)
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 获取查询结果失败! 错误原因: %v", err))
		return err
	}
	return nil
}

func (c *CKHandler) InsertData(query string, data ...interface{}) (bool, error) {
	var err error
	tx, err := c.clickHouseConnect.Begin()
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 创建事务失败! 错误原因: %v", err))
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(query)
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 生成 SQL 预编译语句失败! 错误原因: %v", err))
		return false, err
	}
	if stmt == nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 生成 SQL 预编译对象失败!"))
		return false, errors.New("预编译对象为空")
	}
	if _, execErr := stmt.Exec(data...); execErr != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 数据写入失败! 错误原因: %v", execErr))
		err = execErr
		return false, execErr
	}
	err = tx.Commit()
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 数据写入事务执行失败! 错误原因: %v", err))
		return false, err
	}
	return true, nil
}

func (c *CKHandler) BatchInsertData(query string, dataArrays [][]interface{}) (bool, error) {
	if dataArrays == nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 写入方法缺少参数"))
		return false, errors.New("ClickHouse 写入方法缺少参数")
	}
	var err error
	tx, err := c.clickHouseConnect.Begin()
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 创建事务失败! 错误原因: %v", err))
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(query)
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 生成 SQL 预编译语句失败! 错误原因: %v", err))
		return false, err
	}
	if stmt == nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 生成 SQL 预编译对象失败"))
		return false, errors.New("ClickHouse 生成 SQL 预编译对象失败")
	}
	Logger.Info(GetLogPrefix("") + fmt.Sprintf("READY INSERT %d", len(dataArrays)))
	for _, data := range dataArrays {
		if _, execErr := stmt.Exec(data...); execErr != nil {
			Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 数据写入失败! 错误原因: %v", execErr))
			err = execErr
			return false, execErr
		}
	}
	err = tx.Commit()
	if err != nil {
		Logger.Error(GetLogPrefix("") + fmt.Sprintf("ClickHouse 数据写入事务执行失败! 错误原因: %v", err))
		return false, err
	}
	return true, nil
}

func (c *CKHandler) InitClickHouse() {
	// &debug=true
	ckConnect, connectErr := sqlx.Open(
		"clickhouse",
		fmt.Sprintf(
			"tcp://%s:%d?username=%s&password=%s&database=%s&block_size=%d&read_timeout=%d",
			c.Host, c.Port, c.Username, url.QueryEscape(c.Password), c.Database, ClientMaxBlockSize, ClientReadTimeout))
	if connectErr != nil {
		Logger.Fatal(GetLogPrefix("") + fmt.Sprintf("ClickHouse 创建连接对象失败! 错误原因: %v", connectErr))
	}
	if pingErr := ckConnect.Ping(); pingErr != nil {
		if exception, ok := pingErr.(*clickhouse.Exception); ok {
			Logger.Fatal(GetLogPrefix("") + fmt.Sprintf("ClickHouse 连接失败! 错误原因: %v; [%d] %s; %s\n", connectErr, exception.Code, exception.Message, exception.StackTrace))
		} else {
			Logger.Fatal(GetLogPrefix("") + fmt.Sprintf("ClickHouse 连接失败! 错误原因: %v", pingErr))
		}
	}
	c.clickHouseConnect = ckConnect
	Logger.Info(GetLogPrefix("") + fmt.Sprintf("ClickHouse 连接成功!"))
}
func (c *CKHandler) InitInsertSQL() {
	keys := ""
	values := ""
	for i, v := range c.DataSchema {
		if i < len(c.DataSchema)-1 {
			keys += v + ","
			values += "?,"
		} else {
			keys += v
			values += "?"
		}
	}
	c.InsertSql = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		C.Database,
		c.Table, keys, values)
	//c.InsertSql = fmt.Sprintf("INSERT INTO %s.%s (*) VALUES (%s)",
	//	utils.ConfigJson.ClickHouseConfig.Database,
	//	utils.ConfigJson.ClickHouseConfig.Table,
	//	preStmtSubSQL,
	//)
}

func NewCKHandler(ckConf *ClickhouseConf) *CKHandler {
	client := &CKHandler{
		ClickhouseConf{
			Host:       ckConf.Host,
			Port:       ckConf.Port,
			Username:   ckConf.Username,
			Password:   ckConf.Password,
			Database:   ckConf.Database,
			Table:      ckConf.Table,
			DataSchema: ckConf.DataSchema,
		},
		nil,
		"",
	}
	client.InitClickHouse()
	client.InitInsertSQL()
	return client
}
