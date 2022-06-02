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

var packagePath string

func init() {
	flag.StringVar(&packagePath, "package-path", "", "path to the integration package")
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
func packageInfo(packagePath string) (string, string, error) {
	var name string
	var version string

	mFile, err := os.Open(path.Join(packagePath, "manifest.yml"))
	if err != nil {
		return name, version, fmt.Errorf("error opening manifest %v", err)
	}
	defer mFile.Close()
	mBytes, err := ioutil.ReadAll(mFile)
	if err != nil {
		return name, version, fmt.Errorf("error reading manifest %v", err)
	}
	namePath, _ := yaml.PathString("$.name")
	versionPath, _ := yaml.PathString("$.version")
	manifest := bytes.NewReader(mBytes)
	namePath.Read(manifest, &name)
	manifest.Seek(0, io.SeekStart)
	versionPath.Read(manifest, &version)
	return name, version, nil
}

func main() {
	flag.Parse()
	if packagePath == "" {
		_, usage := flag.UnquoteUsage(flag.Lookup("package-path"))
		fmt.Printf("missing: %s\n", usage)
		os.Exit(1)
	}
	err := validPackage(packagePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	name, version, err := packageInfo(packagePath)
	log.Printf("Package (%s), Version (%s)\n", name, version)
	datastreams, err := ioutil.ReadDir(path.Join(packagePath, "data_stream"))
	if err != nil {
		fmt.Printf("unable to read data stream directory: %v\n", err)
		os.Exit(1)
	}
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
}
