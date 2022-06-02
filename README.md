# To ECS

A tool that scans an integration package's fields and pipelines and makes recommendations on moving custom package specific fields to ECS fields.

## Build

```
go build ./...
```

## Install

```
go install ./...
```

## Usage

```
$ toecs -package-path <path-to-integration-package-directory>
```

## Limitations 

- Not all field properties are recognized from the [spec](https://github.com/elastic/package-spec/blob/main/versions/1/integration/data_stream/fields/fields.spec.yml). The tool currently reads only field name, value and type.
