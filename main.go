package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/go-chi/chi"
	_ "github.com/go-sql-driver/mysql"
)

// AppConfiguration of main application
type AppConfiguration struct {
	Address  string `json:"address"`
	Port     int    `json:"port"`
	MysqlDSN string `json:"mysql-dsn"`
}

// MysqlSlaveInfo data from mysql
type MysqlSlaveInfo struct {
	MasterLogFile       string
	SlaveIORunning      string
	SlaveSQLRunning     string
	SecondsBehindMaster int
	LastErrno           int
	LastIOErrno         int
	LastSQLErrno        int
}

// SlaveInfoSender struct needed for methods
type SlaveInfoSender struct {
	config *AppConfiguration
}

// NewSlaveInfoSender inits info sender
func NewSlaveInfoSender(conf AppConfiguration) SlaveInfoSender {
	return SlaveInfoSender{&conf}
}

func (sis SlaveInfoSender) handlerWriter(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open("mysql", sis.config.MysqlDSN)
	if err != nil {
		fmt.Println(err)
		w.Write([]byte("err: connection"))
		return
	}
	defer db.Close()

	rows, err := db.Query("SHOW SLAVE STATUS")
	if err != nil {
		fmt.Println(err)
		w.Write([]byte("err: query"))
		return
	}
	defer rows.Close()

	// https://github.com/go-sql-driver/mysql/wiki/Examples
	cols, err := rows.Columns()
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	slaveInfo := MysqlSlaveInfo{}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			fmt.Println(err)
			w.Write([]byte("err: row scan"))
		}

		for i, col := range values {
			if col != nil {
				switch cols[i] {
				case "Master_Log_File":
					slaveInfo.MasterLogFile = string(col)
				case "Slave_IO_Running":
					slaveInfo.SlaveIORunning = string(col)
				case "Slave_SQL_Running":
					slaveInfo.SlaveSQLRunning = string(col)
				case "Seconds_Behind_Master":
					slaveInfo.SecondsBehindMaster, err = strconv.Atoi(string(col))
					if err != nil {
						fmt.Println(err)
						w.Write([]byte("err: convert"))
						return
					}
				case "Last_IO_Errno":
					slaveInfo.LastIOErrno, err = strconv.Atoi(string(col))
					if err != nil {
						fmt.Println(err)
						w.Write([]byte("err: convert"))
						return
					}
				case "Last_SQL_Errno":
					slaveInfo.LastSQLErrno, err = strconv.Atoi(string(col))
					if err != nil {
						fmt.Println(err)
						w.Write([]byte("err: convert"))
						return
					}
				case "Last_Errno":
					slaveInfo.LastErrno, err = strconv.Atoi(string(col))
					if err != nil {
						fmt.Println(err)
						w.Write([]byte("err: convert"))
						return
					}
				}
			}
		}
		if err = rows.Err(); err != nil {
			fmt.Println(err)
			w.Write([]byte("err: getting rows"))
		}

		if slaveInfo.SlaveIORunning == "Yes" &&
			slaveInfo.SlaveSQLRunning == "Yes" &&
			slaveInfo.SecondsBehindMaster == 0 &&
			slaveInfo.LastIOErrno == 0 &&
			slaveInfo.LastSQLErrno == 0 {
			w.Write([]byte(slaveInfo.MasterLogFile))
		} else {
			w.Write([]byte("err: server not ready"))
		}

	}
}

func main() {
	configPath := flag.String("c", "config.json", "path to configuration file")
	flag.Parse()

	sis := NewSlaveInfoSender(AppConfiguration{
		Address:  "127.0.0.1",
		Port:     12345,
		MysqlDSN: "root:123456@tcp(127.0.0.1:3306)/",
	})

	file, err := os.Open(*configPath)
	if err != nil {
		fmt.Println(fmt.Sprintf("cannot open file: %s, use default values", *configPath))
	} else {
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&sis.config)
		if err != nil {
			panic(err)
		}
	}

	r := chi.NewRouter()
	r.Get("/current-bin-log", sis.handlerWriter)

	http.ListenAndServe(
		fmt.Sprintf("%s:%d", sis.config.Address, sis.config.Port), r)
}
