# API Conversion CLI Tool

With Strimzi's CRD evolving, there needs to be a way to easily migrate existing custom resource configuration to the new CRD schema.
That is why we introduced the `v1beta2` version of the schema, which removes several deprecated fields.
To make it as easy as possible to convert existing Strimzi custom resources, a conversion tool is provided.

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
  convert-file, cf      Convert custom resources from a YAML file
  convert-resource, cr  Convert custom resources directly in Kubernetes
```

## Converting YAML files with custom resources

One way to use the tool is to convert YAML files. 
The converted YAML files can be applied to your Kubernetes cluster either directly or for example using GitOps mechanism.
The input YAML file is specified using the `--file` option.

By default, the converted YAML is printed to standard output.
You can use the `--in-place` option to write the converted YAML into the original source file.
Alternatively, you can create a new YAML file for the conversion using the `--output` option.

The following example commands show how the tool is used:

```
# Convert input.yaml and print the converted resource to standard output
bin/api-conversion.sh convert-file --file input.yaml

# Read resources from input.yaml, convert it, and store it in input.yaml
bin/api-conversion.sh convert-file --file input.yaml --in-place

# Read resources from input.yaml, convert it, and store it in output.yaml
bin/api-conversion.sh convert-file --file input.yaml --output output.yaml
```

The convertor supports multi-document YAMLs.
When your input YAML contains multiple Strimzi custom resources, they are all converted.
If the input YAML also contains any other Kubernetes resources, they are kept in the output file without being modified.

You can also list the help for the file conversion:

```
> bin/api-conversion.sh help convert-file
Usage: bin/api-conversion.sh convert-file [-d] -f=<inputFile> [-ll=<level>]
       [-o=<outputFile> | [--in-place]]
Convert Custom Resources from YAML file
  -d, --debug              Runs the tool in debug mode
  -f, --file=<inputFile>   Specifies the YAML file for the custom resource being converted
      --in-place           Applies the changes directly to the input file
                             specified by --file
      -ll, --log-level=<level>
                           Sets the log level to enable logging
  -o, --output=<outputFile>
                          Creates an output YAML file for the converted custom resource
```

## Converting Kubernetes resources

You can also use the tool to convert Strimzi custom resources directly in your Kubernetes cluster.

You can use the `--kind` option to specify one or more kinds of Strimzi resources.
If you do not specify any kind, the tool will convert Strimzi resources of all kinds.

The `--namespace` option targets the custom resources in a specified namespace for conversion.
You can also use `--all-namespace` to convert resources in the whole Kubernetes cluster.
If  `--namespace` and `--all-namespace` are not specified, your current namespace is used.

If you want to convert only one resource, you can also use a combination of `--name` and `--kind` options.

The following example commands show how the tool is used:

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
