package internal

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func (m *Migrator) createMigrationSchema(db *sql.DB) error {
	_, err := db.Exec("create table if not exists migration ( id bigserial primary key, rank int4 not null unique, name varchar(200) not null unique, checksum varchar(80) not null unique, applied_at timestamp not null)")
	return err
}

func markMigrationApplied(tx *sql.Tx, migration Migration) error {

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

func applyMigration(tx *sql.Tx, migration Migration) error {

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

func createMigrationSchema(db *sql.DB) error {
	_, err := db.Exec("create table if not exists migration ( id bigserial primary key, rank int4 not null unique, name varchar(200) not null unique, checksum varchar(80) not null unique, applied_at timestamp not null)")
	return err
}

func selectAppliedMigrations(tx *sql.Tx) (map[int]AppliedMigration, error) {
	rows, err := tx.Query("select rank, name, checksum from migration order by rank asc for update")
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

func isDbPostgresql(db *sql.DB) error {
	rows, err := db.Query("select version()")
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
