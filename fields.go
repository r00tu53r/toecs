package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/pkg/errors"
)

type fieldsV1 struct {
	Name            string     `yaml:"name,omitempty"`
	Type            string     `yaml:"type,omitempty"`
	Description     string     `yaml:"description,omitempty"`
	Value           string     `yaml:"value,omitempty"`
	MetricType      string     `yaml:"metric_type,omitempty"`
	Unit            string     `yaml:"unit,omitempty"`
	Dimension       bool       `yaml:"dimension,omitempty"`
	Pattern         string     `yaml:"pattern,omitempty"`
	External        string     `yaml:"external,omitempty"`
	DocValues       bool       `yaml:"doc_values,omitempty"`
	Index           bool       `yaml:"index,omitempty"`
	CopyTo          string     `yaml:"copy_to,omitempty"`
	Enabled         bool       `yaml:"enabled,omitempty"`
	Dynamic         bool       `yaml:"dynamic,omitempty"`
	ScalingFactor   int        `yaml:"scaling_factor,omitempty"`
	Analyzer        string     `yaml:"analyzer,omitempty"`
	SearchAnalyzer  string     `yaml:"search_analyzer,omitempty"`
	NullValue       string     `yaml:"null_value,omitempty"`
	IgnoreAbove     int        `yml:"ignore_above,omitempty"`
	ObjectType      string     `yaml:"object_type,omitempty"`
	Path            string     `yaml:"path,omitempty"`
	Normalizer      string     `yaml:"normalizer,omitempty"`
	IncludeInParent bool       `yaml:"include_in_parent,omitempty"`
	IncludeInRoot   bool       `yaml:"include_in_root,omitempty"`
	Fields          []fieldsV1 `yaml:"fields,omitempty"`
	MultiFields     []fieldsV1 `yaml:"multi_fields,omitempty"`
	indent          int
}

func (field fieldsV1) String() string {
	spaces := strings.Repeat(" ", field.indent)
	if field.Fields == nil {
		return fmt.Sprintf("%sname: %s\n%stype: %s\n", spaces, field.Name, spaces, field.Type)
	}
	var s string
	indent := field.indent + 2
	for _, f := range field.Fields {
		f.indent = indent
		s += f.String()
	}
	return s
}

func readFieldFile(path string) ([]fieldsV1, error) {
	var fields []fieldsV1
	fieldsFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fieldsFile.Close()
	content, err := ioutil.ReadAll(fieldsFile)
	if err := yaml.Unmarshal(content, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func readFieldFiles(packagePath string, datastream string) ([]fieldsV1, error) {
	var retFields []fieldsV1
	fieldsPath := path.Join(packagePath, "data_stream", datastream, "fields")
	files, err := ioutil.ReadDir(fieldsPath)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		ext := path.Ext(file.Name())
		if ext == ".yml" || ext == ".yaml" {
			stem := strings.Split(file.Name(), ext)[0]
			if stem == "agent" || stem == "base-fields" || stem == "ecs" {
				log.Println("Skipping", file.Name())
				continue
			}
			fields, err1 := readFieldFile(path.Join(fieldsPath, file.Name()))
			if err1 != nil {
				err = errors.Wrap(err1, fmt.Sprintf("error reading fields from %s", file.Name()))
				continue
			}
			retFields = append(retFields, fields...)
		}
	}
	return retFields, err
}
