package internal

import (
	"crypto/md5"
	"fmt"
	fsPkg "io/fs"
	"regexp"
	"sort"
	"strconv"
)

func collectMigrations(paths []string, fs fsPkg.FS) ([]Migration, error) {

	re := regexp.MustCompile("^.*/?V(\\d+)__(.*).sql")

	readDirFs, isReadDirFs := fs.(fsPkg.ReadDirFS)
	if !isReadDirFs {
		return nil, fmt.Errorf("fs not ReadDir capable")
	}

	readFileFs, isReadFileFs := fs.(fsPkg.ReadFileFS)
	if !isReadFileFs {
		return nil, fmt.Errorf("fs not ReadFile capable")
	}

	migrations := make([]Migration, 0)
	for _, basePath := range paths {
		fileInfos, err := readDirFs.ReadDir(basePath)
		if err != nil {
			return nil, err
		}

		for _, fileInfo := range fileInfos {

			match := re.FindStringSubmatch(fileInfo.Name())

			rankStr := match[1]
			rank, err := strconv.Atoi(rankStr)
			if err != nil {
				return nil, err
			}
			name := match[2]

			path := fmt.Sprintf("%s/%s", basePath, fileInfo.Name())
			data, err := readFileFs.ReadFile(path)
			if err != nil {
				return nil, err
			}

			checksum := fmt.Sprintf("{md5}%x", md5.Sum(data))

			m := Migration{rank: rank, name: name, data: data, checksum: checksum}
			migrations = append(migrations, m)
		}
	}

	sort.Sort(MigrationCollection(migrations))

	return migrations, nil
}
