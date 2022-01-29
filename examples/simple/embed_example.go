package main

import (
	"database/sql"
	"github.com/cbuschka/go-dbmigrate"
	_ "github.com/lib/pq"
	"log"
)

func main() {
	connStr := "postgres://dbmigrate:asdfasdf@localhost/dbmigrate?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	config := dbmigrate.NewDefaultMigratorConfig()
	config.Db = db
	config.MigrationPaths = []string{"examples/simple/migrations"}

	migrator, err := dbmigrate.NewMigrator(config)
	if err != nil {
		log.Fatal(err)
	}

	err = migrator.Migrate()
	if err != nil {
		log.Fatal(err)
	}
}
