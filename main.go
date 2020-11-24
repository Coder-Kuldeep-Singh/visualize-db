package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq" //import postgres driver
)

// DBConfig hold the database connection values
type DBConfig struct {
	Host, Password, Name, User, Port string
}

func loadEnv() *DBConfig {
	return &DBConfig{
		Host:     configString("DBHOST"),
		Password: configString("DBPASSWORD"),
		Name:     configString("DBNAME"),
		User:     configString("DBUSER"),
		Port:     configString("DBPORT"),
	}
}

func configString(name string) string {
	return os.Getenv(name)
}

func (db *DBConfig) dcs(connectionType string) string {
	if connectionType == "postgres" {
		return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			db.Host, db.Port, db.User, db.Password, db.Name)
	} else if connectionType == "mysql" {
		return fmt.Sprintf("%s:%s@tcp(%s)/%s", db.User, db.Password, db.Host, db.Name)
	}
	return ""
}

func (db *DBConfig) connect(connectionType string) *sql.DB {
	dbs, err := sql.Open(toLower(connectionType), db.dcs(toLower(connectionType)))
	if err != nil {
		fmt.Printf("Error %s when opening DB\n", err)
		log.Fatalln(err)
	}
	setLimits(dbs)
	return dbs
}
func toLower(str string) string {
	return strings.ToLower(str)
}

func setLimits(db *sql.DB) {
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)
}

func getList(rows *sql.Rows) []string {
	List := []string{}
	for rows.Next() {
		var values string
		err := rows.Scan(&values)
		if err != nil {
			fmt.Println("error to scan rows.")
			fmt.Println(err)
			return nil
		}
		List = append(List, values)
	}
	return List
}

// Databases holds the descriptions of all tables
type Databases struct {
	Database string
	Tables   *[]Table
}

// Table holds the name of the table with the description
type Table struct {
	TableName     string
	DescribeTable *[]DescribeTable
}

// DescribeTable handles the all information about the table
type DescribeTable struct {
	Field, Type, Null, Key, Default, Extra sql.NullString
}

func describeTable(rows *sql.Rows) *[]DescribeTable {
	List := []DescribeTable{}
	for rows.Next() {
		var Field, Type, Null, Key, Default, Extra sql.NullString
		err := rows.Scan(&Field, &Type, &Null, &Key, &Default, &Extra)
		if err != nil {
			fmt.Println("error to scan databases.")
			fmt.Println(err)
			return nil
		}
		List = append(List, DescribeTable{
			Field:   Field,
			Type:    Type,
			Null:    Null,
			Key:     Key,
			Default: Default,
			Extra:   Extra,
		})
	}
	return &List
}

func genQuery(db *sql.DB, query string) *sql.Rows {
	res, err := db.Query(query)
	if err != nil {
		fmt.Printf("error to run query %s\n", query)
		fmt.Println(err)
		return nil
	}
	return res
}

func getDatabaseList(db *sql.DB, query string) []string {
	rows := genQuery(db, query)
	return getList(rows)

}

func execute(db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		fmt.Printf("error to execute query %s\n", query)
		fmt.Println(err)
		return
	}
}

func useDB(db *sql.DB, dbName string) {
	query := fmt.Sprintf("%s", dbName)
	execute(db, query)
}

func getTableList(db *sql.DB, dbName, query, command string) []string {
	useDB(db, command)
	rows := genQuery(db, query)
	return getList(rows)
}

func getTableInfo(db *sql.DB, tableName, query string) *Table {
	rows := genQuery(db, query)
	// tables := describeTable(rows)
	return &Table{
		TableName:     tableName,
		DescribeTable: describeTable(rows),
	}

	// fmt.Printf("TABLES >>>>> %v\n", *tables)
	// for _, field := range *tables {
	// 	fmt.Printf("%s\n", field.Table)
	// }
}

func iterate(characters int, formatter string) string {
	format := ""
	for idx := 0; idx < characters*2; idx++ {
		format += formatter
	}
	return format
}

// FormatingInfo to print formated data
func FormatingInfo(database, formatter string) string {
	return iterate(len(database), formatter)
}

// MYSQL runs statements over mysql database
func MYSQL(db *sql.DB, databases []string) {
	choosedDB := databases[3]
	use := fmt.Sprintf("USE %s", choosedDB)
	showTables := fmt.Sprintf("SHOW TABLES")
	tables := getTableList(db, choosedDB, showTables, use)
	fmt.Println(choosedDB)
	for _, table := range tables {
		query := fmt.Sprintf("DESCRIBE %s", table)
		tableinfo := getTableInfo(db, table, query)
		log.Println(tableinfo)
		fmt.Println()
		fmt.Println()
	}
}

// PSQL runs statements over postgres database
func PSQL(db *sql.DB, databases []string) {
	choosedDB := databases[3]
	use := fmt.Sprintf("SET search_path TO  %s;", databases[11])
	showTables := fmt.Sprintf("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
	tables := getTableList(db, choosedDB, showTables, use)
	fmt.Println(choosedDB)
	for _, table := range tables {
		// fmt.Printf("TABLE %s %s\n", FormatingInfo(databases[13], "-"), table)
		query := fmt.Sprintf("SELECT column_name,data_type,is_nullable,is_identity,column_default,ordinal_position FROM information_schema.columns WHERE table_name = '%s'", table)
		tableinfo := getTableInfo(db, table, query)
		log.Println(tableinfo)
		fmt.Println()
		fmt.Println()
	}
}

func main() {
	mysql := flag.Bool("mysql", false, "enable the flag to activate mysql database")
	psql := flag.Bool("psql", false, "enable the flag to activate postgres database")
	flag.Parse()

	if !*mysql && !*psql {
		flag.CommandLine.Usage()
		log.Fatalln("flags empty")
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file -> ", err)
		return
	}
	if !*mysql {
		dbType := "postgres"
		dbLists := fmt.Sprintf("SELECT datname FROM pg_database;")
		db := loadEnv().connect(dbType)
		databases := getDatabaseList(db, dbLists)
		log.Println(databases)
		PSQL(db, databases)
		defer db.Close()
	}
	if !*psql {
		dbType := "mysql"
		dbLists := fmt.Sprint("SHOW DATABASES")
		db := loadEnv().connect(dbType)
		databases := getDatabaseList(db, dbLists)
		log.Println(databases)
		MYSQL(db, databases)
		defer db.Close()
	}
}
