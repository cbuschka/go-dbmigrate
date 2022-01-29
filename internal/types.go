package internal

import "strings"

type Migration struct {
	rank     int
	name     string
	data     []byte
	checksum string
}

type MigrationCollection []Migration

func (m MigrationCollection) Len() int {
	return len(m)
}

func (m MigrationCollection) Less(i, j int) bool {
	return strings.Compare(m[i].name, m[j].name) < 0
}

func (m MigrationCollection) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

type AppliedMigration struct {
	rank     int
	name     string
	checksum string
}
