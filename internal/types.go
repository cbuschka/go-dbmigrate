package internal

import (
	"fmt"
)

type Migration struct {
	rank     int
	name     string
	data     []byte
	checksum string
}

func (m Migration) String() string {
	return fmt.Sprintf("rank=%d,name=%s,checksum=%s", m.rank, m.name, m.checksum)
}

type MigrationCollection []Migration

func (m MigrationCollection) Len() int {
	return len(m)
}

func (m MigrationCollection) Less(i, j int) bool {
	return m[i].rank < m[j].rank
}

func (m MigrationCollection) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

type AppliedMigration struct {
	rank     int
	name     string
	checksum string
}
