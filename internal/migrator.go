package internal

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"strings"
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

	err := m.isDbPostgresql()
	if err != nil {
		return err
	}

	err = m.createMigrationSchema()
	if err != nil {
		return err
	}

	for {
		hasMore, err := m.advanceMigration()
		if err != nil {
			return err
		}

		if !hasMore {
			break
		}
	}

	return nil
}

func (m *Migrator) markMigrationApplied(tx *sql.Tx, migration Migration) error {

	stmt, err := tx.Prepare("insert into migration ( rank, name, checksum, applied_at ) values ($1, $2, $3, now())")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(migration.rank, migration.name, migration.checksum)
	if err != nil {
		return err
	}

	return nil
}

func (m *Migrator) applyMigration(tx *sql.Tx, migration Migration) error {

	log.Printf("Applying migration rank=%d, name=%s, checksum=%s...", migration.rank, migration.name, migration.checksum)

	sqlRd := NewSqlReader(bytes.NewReader(migration.data))
	statements, err := sqlRd.ReadStatements()
	if err != nil {
		return err
	}

	for _, statement := range statements {

		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}

		log.Printf("Executing '%s'...", statement)

		_, err := tx.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) createMigrationSchema() error {
	_, err := m.db.Exec("create table if not exists Migration ( id bigserial primary key, rank int4 not null unique, name varchar(200) not null unique, checksum varchar(80) not null unique, applied_at timestamp not null)")
	return err
}

func (m *Migrator) selectAppliedMigrations(tx *sql.Tx) (map[int]AppliedMigration, error) {
	rows, err := tx.Query("select rank, name, checksum from Migration order by rank asc for update")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	appliedMigrations := make(map[int]AppliedMigration, 0)
	for rows.Next() {
		var rank int
		var name string
		var checksum string
		err := rows.Scan(&rank, &name, &checksum)
		if err != nil {
			return nil, err
		}
		appliedMigrations[rank] = AppliedMigration{rank: rank, name: name, checksum: checksum}
	}

	return appliedMigrations, nil
}

func (m *Migrator) isDbPostgresql() error {
	rows, err := m.db.Query("select version()")
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("no rows")
	}

	var version string
	err = rows.Scan(&version)
	if err != nil {
		return err
	}

	version = strings.ToLower(version)
	if !strings.Contains(version, "postgres") {
		return fmt.Errorf("only postgresql supported")
	}

	return nil
}

func (m *Migrator) advanceMigration() (bool, error) {

	tx, err := m.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	appliedMigrations, err := m.selectAppliedMigrations(tx)
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
		} else {

			err = m.applyMigration(tx, migration)
			if err != nil {
				return false, err
			}

			err = m.markMigrationApplied(tx, migration)
			if err != nil {
				return false, err
			}

			err = tx.Commit()
			if err != nil {
				return false, err
			}

			return true, nil
		}
	}

	return false, nil
}
