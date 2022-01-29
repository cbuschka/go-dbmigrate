package internal

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestReadsEmptyScript(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader(""))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{}, statements)
}

func TestReadsSingleStatement(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader("select version()"))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{"select version()"}, statements)
}

func TestReadsSingleStatementTerminateWithSemicolon(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader("select version();"))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{"select version()"}, statements)
}

func TestReadsMultipleStatementsSeperatedWithSemicolons(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader("select version(); select2 version2();"))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{"select version()", "select2 version2()"}, statements)
}

func TestReadsSingleStatementWithSemicolonInStringValue(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader("select ';'"))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{"select ';'"}, statements)
}

func TestReadsSingleStatementWithSemicolonAndDoubleQuoteInStringValue(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader("select ''';'"))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{"select ''';'"}, statements)
}

func TestReadsSingleStatementWithSemicolonInIdentifier(t *testing.T) {
	sqlRd := NewSqlReader(strings.NewReader("select \";\""))

	statements, err := sqlRd.ReadStatements()
	if err != nil {
		t.Fatal(err)
		return
	}

	assert.Equal(t, []string{"select \";\""}, statements)
}
