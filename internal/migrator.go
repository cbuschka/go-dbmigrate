package internal

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log"
)

type Migrator struct {
	db             *sql.DB
	fs             fs.FS
	migrationPaths []string
}

func NewMigrator(db *sql.DB, fs fs.FS, migrationPaths []string) (*Migrator, error) {
	return &Migrator{db: db, fs: fs, migrationPaths: migrationPaths}, nil
}

func (m *Migrator) Migrate() error {

	err := isDbPostgresql(m.db)
	if err != nil {
		return err
	}

	log.Print("Migrating migration schema...")

	err = m.createMigrationSchema(m.db)
	if err != nil {
		return err
	}

	log.Print("Migrating database...")

	for {
		hasMore, err := m.advanceMigration()
		if err != nil {
			return err
		}

		if !hasMore {
			break
		}
	}

	log.Print("Migrating finished.")

	return nil
}

func (m *Migrator) advanceMigration() (bool, error) {

	tx, err := m.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	appliedMigrations, err := selectAppliedMigrations(tx)
	if err != nil {
		return false, err
	}

	migrations, err := collectMigrations(m.migrationPaths, m.fs)
	if err != nil {
		return false, err
	}

	for _, migration := range migrations {
		appliedMigration, migrationApplied := appliedMigrations[migration.rank]
		if migrationApplied {
			if appliedMigration.checksum != migration.checksum {
				return false, fmt.Errorf("checksum mismatch")
			}

			if appliedMigration.name != migration.name {
				return false, fmt.Errorf("name mismatch")
			}

			log.Printf("Migration %s OK.", migration.String())
		} else {

			log.Printf("Applying migration %s...", migration.String())

			err = applyMigration(tx, migration)
			if err != nil {
				return false, err
			}

			err = markMigrationApplied(tx, migration)
			if err != nil {
				return false, err
			}

			err = tx.Commit()
			if err != nil {
				return false, err
			}

			log.Printf("Migration %s successfully applied.", migration.String())

			return true, nil
		}
	}

	return false, nil
}
