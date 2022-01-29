package dbmigrate

import (
	"database/sql"
	"github.com/cbuschka/go-dbmigrate/internal"
	"io/fs"
	"os"
)

type Migrator interface {
	Migrate() error
}

func Migrate(db *sql.DB) error {

	config := NewDefaultMigratorConfig()
	config.Db = db

	migrator, err := NewMigrator(config)
	if err != nil {
		return err
	}

	return migrator.Migrate()
}

type MigratorConfig struct {
	Fs             fs.FS
	Db             *sql.DB
	MigrationPaths []string
}

func NewDefaultMigratorConfig() *MigratorConfig {
	return &MigratorConfig{Db: nil, Fs: nil, MigrationPaths: []string{"migrations"}}
}

func NewMigrator(config *MigratorConfig) (Migrator, error) {

	fs := config.Fs
	if fs == nil {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}

		fs = os.DirFS(cwd)
	}

	migrator, err := internal.NewMigrator(config.Db, fs, config.MigrationPaths)
	if err != nil {
		return nil, err
	}

	return Migrator(migrator), nil
}
