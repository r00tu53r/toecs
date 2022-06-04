package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/elastic/package-spec/code/go/pkg/validator"
	"github.com/goccy/go-yaml"
)

var (
	packagePath string
	ecsGitRef   string
)

func init() {
	flag.StringVar(&packagePath, "package-path", "", "path to the integration package")
	flag.StringVar(&ecsGitRef, "ecs-git-ref", "main", "git tag / branch on ECS repo")
}

func validPackage(packagePath string) error {
	fileInfo, err := os.Stat(packagePath)
	if err != nil {
		return fmt.Errorf("invalid package path: %v", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("%s must be a valid integration package directory", packagePath)
	}
	err = validator.ValidateFromPath(packagePath)
	if err != nil {
		return fmt.Errorf("%s must be a valid integration package: %v", packagePath, err)
	}
	return nil
}

// packageInfo returns the name, version and categories for
// the given package path.
func packageInfo(packagePath string) (string, string, []string, error) {
	var name string
	var version string
	var categories []string

	mFile, err := os.Open(path.Join(packagePath, "manifest.yml"))
	if err != nil {
		return name, version, categories, fmt.Errorf("error opening manifest %v", err)
	}
	defer mFile.Close()
	mBytes, err := ioutil.ReadAll(mFile)
	if err != nil {
		return name, version, categories, fmt.Errorf("error reading manifest %v", err)
	}
	namePath, _ := yaml.PathString("$.name")
	versionPath, _ := yaml.PathString("$.version")
	catPath, _ := yaml.PathString("$.categories")
	manifest := bytes.NewReader(mBytes)
	namePath.Read(manifest, &name)
	manifest.Seek(0, io.SeekStart)
	versionPath.Read(manifest, &version)
	manifest.Seek(0, io.SeekStart)
	catPath.Read(manifest, &categories)
	return name, version, categories, nil
}

func main() {
	flag.Parse()
	if packagePath == "" || ecsGitRef == "" {
		_, packagePathUsage := flag.UnquoteUsage(flag.Lookup("package-path"))
		_, gitRefUsage := flag.UnquoteUsage(flag.Lookup("ecs-git-ref"))
		fmt.Printf("missing:\n%s\n%s\n", packagePathUsage, gitRefUsage)
		os.Exit(1)
	}
	err := validPackage(packagePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	name, version, categories, err := packageInfo(packagePath)
	log.Printf("Package (%s), Version (%s), Categories: (%v)\n", name, version, categories)
	datastreams, err := ioutil.ReadDir(path.Join(packagePath, "data_stream"))
	if err != nil {
		fmt.Printf("unable to read data stream directory: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Loading fields")
	var fields []fieldV1
	for _, ds := range datastreams {
		if ds.IsDir() {
			log.Println("Processing data stream", ds.Name())
			fields, err = readFieldFiles(packagePath, ds.Name())
			for {
				err1 := errors.Unwrap(err)
				if err1 == nil {
					break
				}
				log.Println(err1)
			}
			if fields != nil {
				for i, v := range fields {
					log.Printf("field[%d]: %v", i, v)
				}
			}
		}
	}
	log.Println("Flattened fields")
	for i, f := range fields {
		log.Printf("flattened[%d]: %v", i, f.Flatten())
	}
	fmt.Println("Loading ECS schema")
	schema, err := loadECSSchema(ecsGitRef)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Schema loaded")
	for k, v := range schema {
		log.Printf("%s -> %v", k, v)
	}
}
