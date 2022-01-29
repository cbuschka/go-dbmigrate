package dbmigrate

import (
	"database/sql"
	"github.com/cbuschka/go-dbmigrate/internal"
	"os"
)

func Migrate(db *sql.DB) error {

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	defaultFs := os.DirFS(cwd)

	migrator, err := internal.NewMigrator(db, defaultFs)
	if err != nil {
		return err
	}

	err = migrator.Migrate()
	if err != nil {
		return err
	}

	return nil
}
