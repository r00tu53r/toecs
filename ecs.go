package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
)

var (
	ecsSchemaURL       = "https://raw.githubusercontent.com/elastic/ecs/%s/generated/ecs/%s"
	ecsSchemaFile      = "ecs_flat.yml"
	cachedECSSchemaDir = ".cache/toecs"
)

func cacheECSSchema(ecsGitRef string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("%w: unable to cache ECS schema", err)
	}
	cacheDir := path.Join(home, cachedECSSchemaDir, ecsGitRef)
	fileInfo, err := os.Stat(cacheDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(cacheDir, 0750)
			if err != nil {
				return fmt.Errorf("%w: unable to create cache dir %s", cacheDir)
			}
		} else {
			return err
		}
	}
	if fileInfo != nil && !fileInfo.IsDir() {
		return fmt.Errorf("%s must be a directory", cacheDir)
	}
	schemaFilename := path.Join(cacheDir, ecsSchemaFile)
	fileInfo, err = os.Stat(schemaFilename)
	if err != nil {
		log.Println("Downloading ECS schema file for", ecsGitRef)
		content, err := downloadECSSchemaFile(ecsGitRef)
		if err != nil {
			return fmt.Errorf("%w: unable to cache ECS schema")
		}
		schemaFile, err := os.Create(schemaFilename)
		if err != nil {
			return fmt.Errorf("%w: unable to create schema file %s", schemaFilename)
		}
		if _, err = schemaFile.Write(content); err != nil {
			return fmt.Errorf("%w: unable to write to schema file %s", schemaFilename)
		}
	}
	return nil
}

func downloadECSSchemaFile(ecsGitRef string) ([]byte, error) {
	url := fmt.Sprintf(ecsSchemaURL, ecsGitRef, ecsSchemaFile)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("%w: can't download the online schema (URL: %s)", url)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("unsatisfied ECS dependency (URL: %s)", url)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status code: %d", resp.StatusCode)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: can't read schema content (URL: %s)", url)
	}
	return content, nil
}
