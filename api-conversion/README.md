# API Conversion CLI Tool

With Strimzi's CRD evolving, there needs to be a way to easily migrate existing CR configuration to the new CRD schema.
That is why we introduced the `v1beta2` version of the schema which removes several deprecated fields.
To make it as easy as possible to convert the existing custom resources, we provide this conversion tool.

The tool can operate in two modes:
* Converting YAML files
* Converting Kubernetes resources

You can list the available features using the `help` command:

```
> bin/api-conversion.sh help
Usage: bin/api-conversion.sh [-hV] [COMMAND]
Conversion tool for Strimzi Custom Resources
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help                  Displays help information about the specified command
  convert-file, cf      Convert Custom Resources from YAML file
  convert-resource, cr  Convert Custom Resources directly in Kubernetes
```

## Converting YAML files with custom resources

One way to use the tool is to convert YAML files. 
The converted YAML files can be applied to your Kubernetes cluster either directly or for example using GitOps mechanism.
The input YAML file is specified using the `--file` option.

The converted YAML will be by default printed to standard output.
You can use the additional options `--in-place` to write the converted YAML into the original source file.
You can also specify another output file using the `--output` option.

Following example commands show how the tool can be used:

```
# Convert input.yaml and print the converted resource to standard output
bin/api-conversion.sh convert-file --file input.yaml

# Read resources from input.yaml, convert it, and store it in input.yaml
bin/api-conversion.sh convert-file --file input.yaml --in-place

# Read resources from input.yaml, convert it, and store it in output.yaml
bin/api-conversion.sh convert-file --file input.yaml --output output.yaml
```

The convertor supports multi-document YAMLs.
When your input YAML contains multiple Strimzi custom resources, they will be all converted.
If the input YAL also contains any other Kubernetes resources, they will be kept in the output file without being modified.

You can also list the help for the file conversion:

```
> bin/api-conversion.sh help convert-file
Usage: bin/api-conversion.sh convert-file [-d] -f=<inputFile> [-ll=<level>]
       [-o=<outputFile> | [--in-place]]
Convert Custom Resources from YAML file
  -d, --debug              Use debug
  -f, --file=<inputFile>   The YAML file with the Strimzi Custom Resource which
                             should be converted
      --in-place           Apply the changes directly to the input file
                             specified by --file
      -ll, --log-level=<level>
                           Set log level to enable logging
  -o, --output=<outputFile>
                           The output YAML file with the converted Strimzi
                             Custom Resource
```

## Converting Kubernetes resources

You can also use the tool to convert Strimzi custom resources directly in your Kubernetes cluster.

You can use the `--kind` option to specify one or more kinds of Strimzi resources.
If you do not specify any Kind, the tool will convert resources of all kinds.

The `--namespace` option can be used to specify a namespace in which the resources should be converted.
You can also use `--all-namespace` to convert resources in the whole Kubernetes cluster.
When neither `--namespace` nor `--all-namespace` is specified your current namespace will used.

If you want to convert only one resource, you can also use the combination of `--name` and `--kind` options.

Following example commands show how the tool can be used:

```
# Converts all Strimzi resources in current namespace
> bin/api-conversion.sh convert-resource

# Converts all Strimzi resources in all namespace
> bin/api-conversion.sh convert-resource --all-namespaces

# Converts all Strimzi resources in namespace my-kafka
> bin/api-conversion.sh convert-resource --namespaces my-kafka

# Convert only Kafka resources in all namespaces
> bin/api-conversion.sh convert-resource --all-namespaces --kind Kafka

# Convert only Kafka and Kafka Connect resources in all namespaces
> bin/api-conversion.sh convert-resource --all-namespaces --kind Kafka --kind KafkaConnect

# Convert Kafka cluster named my-cluster in namespace my-kafka
> bin/api-conversion.sh convert-resource --kind Kafka --namespace my-kafka --name my-cluster
```

You can also list the help for the resource conversion:

```
> bin/api-conversion.sh help convert-resource
Usage: bin/api-conversion.sh convert-resource [-d] [-ll=<level>]
       [--name=<name>] [-k[=<kinds> [<kinds> [<kinds> [<kinds> [<kinds>
       [<kinds> [<kinds> [<kinds> [<kinds> [<kinds>]]]]]]]]]]]...
       [-n=<namespace> | [-a]]
Convert Custom Resources directly in Kubernetes
  -a, --all-namespaces   Convert resources in all namespaces
  -d, --debug            Use debug?
  -k, --kind[=<kinds> [<kinds> [<kinds> [<kinds> [<kinds> [<kinds> [<kinds>
        [<kinds> [<kinds> [<kinds>]]]]]]]]]]
                         Resource Kind which should be converted (if not
                           specified, all Strimzi resources will be converted)
      -ll, --log-level=<level>
                         Set log level to enable logging
  -n, --namespace=<namespace>
                         Kubernetes namespace / OpenShift project (if not
                           specified, current namespace will be used)
      --name=<name>      Name of the resource which should be converted (can be
                           used onl with --namespace and single --kind options)
```