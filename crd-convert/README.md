# CRD conversion

With Strimzi's CRD evolving, there needs to be a way to easily migrate existing CR configuration to the new CRD schema.
That is why we introduced the `v1beta2` version of the schema whicih removes several deprecated fields.
To make it as easy as possible to convert the existing custom resources, we provide this conversion tool.

The tool can operate in two modes:
* Converting YAML files
* Converting Kubernetes resources

You can list the available features using the `help` command:

```
> bin/crd-convert.sh help
Usage: bin/crd-convert.sh [-hV] [COMMAND]
Conversion tool for Strimzi Custom Resources
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help                  Displays help information about the specified command
  convert-file, cf      Convert Custom Resources from YAML file
  convert-resource, cr  Convert Custom Resources directly in Kubernetes

```

## Converting YAML files with custom resources

One way to use the tool is to convert YAML files. The converted YAML files can be applied to your Kubernetes cluster either directly or using GitOps mechanism etc.
The input file is specified using the `--file` option.
The output file where the converted YAML will be written can be specified in `-output`.
If no output is specified, the converted YAML will be written directly into the input file.

The convertor supports multi-document YAMLs.
When your input YAML contains multiple Strimzi custom resources, they will be all converted.
If the input YAL also contains any other Kubernetes resources, they will be kept in the output file without being modified.

Following example shows how to convert a resource from YAML file:

```
> bin/crd-convert.sh convert-file --file input.yaml --output output.yaml
```

You can also list the help for the file convertor:

```
> bin/crd-convert.sh help help convert-file
Usage: bin/crd-convert.sh convert-file [-d] -f=<inputFile> [-ll=<level>]
                                       [-o=<outputFile>]
Convert Custom Resources from YAML file
  -d, --debug              Use debug?
  -f, --file=<inputFile>   The YAML file with the Strimzi Custom Resource which
                             should be converted
      -ll, --log-level=<level>
                           Set log level to enable logging
  -o, --output=<outputFile>
                           The YAML file with the converted Strimzi Custom
                             Resource (if not specified, the input file will be
                             overwritten)
```

## Converting Kubernetes resources

TODO