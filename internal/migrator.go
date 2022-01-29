package internal

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Migrator struct {
	db *sql.DB
	fs fs.FS
}

func NewMigrator(db *sql.DB, fs fs.FS) (*Migrator, error) {
	return &Migrator{db: db, fs: fs}, nil
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

	appliedMigrations, err := m.selectAppliedMigrations()
	if err != nil {
		return err
	}

	migrations, err := m.loadMigrations()
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		appliedMigration, migrationApplied := appliedMigrations[migration.rank]
		if migrationApplied {
			if appliedMigration.checksum != migration.checksum {
				return fmt.Errorf("checksum mismatch")
			}

			if appliedMigration.name != migration.name {
				return fmt.Errorf("name mismatch")
			}
		} else {

			tx, err := m.db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			err = m.applyMigration(tx, migration)
			if err != nil {
				return err
			}

			err = m.markMigrationApplied(tx, migration)
			if err != nil {
				return err
			}

			err = tx.Commit()
			if err != nil {
				return err
			}
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

	log.Printf("Applying migration %d %s %s", migration.rank, migration.name, migration.checksum)

	script := string(migration.data)
	statements := strings.Split(strings.ReplaceAll(script, "\r\n", "\n"), "/\n")

	for _, statement := range statements {

		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}

		log.Printf("Executing '%s'", statement)

		_, err := tx.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) loadMigrations() ([]Migration, error) {

	re := regexp.MustCompile("^.*/?V(\\d+)__(.*).sql")

	basePath := "migrations"
	fileInfos, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	migrations := make([]Migration, 0)
	for _, fileInfo := range fileInfos {

		match := re.FindStringSubmatch(fileInfo.Name())

		rankStr := match[1]
		rank, err := strconv.Atoi(rankStr)
		if err != nil {
			return nil, err
		}
		name := match[2]

		path := fmt.Sprintf("%s/%s", basePath, fileInfo.Name())
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}

		checksum := fmt.Sprintf("{md5}%x", md5.Sum(data))

		m := Migration{rank: rank, name: name, data: data, checksum: checksum}
		migrations = append(migrations, m)
	}

	sort.Sort(MigrationCollection(migrations))

	return migrations, nil
}

func (m *Migrator) createMigrationSchema() error {
	_, err := m.db.Exec("create table if not exists Migration ( id bigserial primary key, rank int4 not null unique, name varchar(200) not null unique, checksum varchar(80) not null unique, applied_at timestamp not null)")
	return err
}

func (m *Migrator) selectAppliedMigrations() (map[int]AppliedMigration, error) {
	rows, err := m.db.Query("select rank, name, checksum from Migration order by rank asc")
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
		return fmt.Errorf("only postgresql support")
	}

	return nil
}
