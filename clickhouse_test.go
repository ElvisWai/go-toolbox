package go_toolbox

import (
	"fmt"
	"testing"
)

func TestClickhouse(t *testing.T) {

	dataSchema := []string{
		"id",
		"username",
		"geo",
	}
	config := ClickhouseConf{
		Host:       "",
		Port:       19000,
		Username:   "",
		Password:   "",
		Database:   "group",
		Table:      "user_geo",
		DataSchema: dataSchema,
	}
	clickhouseHandler := NewCKHandler(&config)
	keys := ""
	values := ""
	for i, v := range clickhouseHandler.DataSchema {
		if i < len(clickhouseHandler.DataSchema)-1 {
			keys += v + ","
			values += "?,"
		} else {
			keys += v
			values += "?"
		}
	}
	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		config.Database,
		config.Table, keys, values)

	fmt.Println(keys)
	fmt.Println(sql)
}
