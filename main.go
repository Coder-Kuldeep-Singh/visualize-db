package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
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

func (db *DBConfig) dcs() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", db.User, db.Password, db.Host, db.Name)
}

func (db *DBConfig) connect() *sql.DB {
	dbs, err := sql.Open("mysql", db.dcs())
	if err != nil {
		fmt.Printf("Error %s when opening DB\n", err)
		log.Fatalln(err)
	}
	setLimits(dbs)
	return dbs
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
			fmt.Println("error to scan databases.")
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
		fmt.Printf("error to run query %s", query)
		fmt.Println(err)
		return nil
	}
	return res
}

func getDatabaseList(db *sql.DB) []string {
	rows := genQuery(db, "SHOW DATABASES")
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
	query := fmt.Sprintf("USE %s", dbName)
	execute(db, query)
}

func getTableList(db *sql.DB, dbName string) []string {
	useDB(db, dbName)
	rows := genQuery(db, "SHOW TABLES")
	return getList(rows)
}

func getTableInfo(db *sql.DB, tableName string) *Table {
	rows := genQuery(db, "DESCRIBE "+tableName)
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

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file -> ", err)
		return
	}
	db := loadEnv().connect()
	// nbj := []NearByJobs{}
	databases := getDatabaseList(db)
	tables := getTableList(db, databases[13])
	fmt.Println(databases[13])
	for _, table := range tables {
		// fmt.Printf("TABLE %s %s\n", FormatingInfo(databases[13], "-"), table)
		tableinfo := getTableInfo(db, table)
		log.Println(tableinfo)
		fmt.Println()
		fmt.Println()
	}

	defer db.Close()
}
