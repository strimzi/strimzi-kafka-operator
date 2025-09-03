# `v1` API Conversion CLI Tool

With Strimzi's CRD evolving, there needs to be a way to easily migrate existing custom resource configuration to the new CRD schema.
That is why we introduced the `v1` version of the schema, which removes several deprecated fields.
To make it as easy as possible to convert existing Strimzi custom resources, a conversion tool is provided.

The tool can operate in two modes:
* Converting YAML files
* Converting Kubernetes resources
* Upgrading CRDs to v1

You can list the available features using the `help` command:

```
> bin/api-conversion.sh help
Usage: bin/api-conversion.sh [-hV] [COMMAND]
Conversion tool for Strimzi Custom Resources
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help                                     Displays help information about the specified command
  convert-file, cf                         Convert Custom Resources from YAML file
  convert-resource, cr, convert-resources  Convert Custom Resources directly in Kubernetes
  crd-upgrade, crd                         Upgrades the Strimzi CRDs and CRs to use v1beta2 version
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

**When you have converted the YAML files, apply the changes to Kubernetes using `kubectl apply -f` or `kubectl replace -f`.** 

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
  -a, --all-namespaces   Converts resources in all namespaces
  -d, --debug            Runs the tool in debug mode
  -k, --kind[=<kinds> [<kinds> [<kinds> [<kinds> [<kinds> [<kinds> [<kinds>
        [<kinds> [<kinds> [<kinds>]]]]]]]]]]
                         Specifies the kinds of custom resources to be
                           converted, or converts all resources if not specified
      -ll, --log-level=<level>
                         Sets the log level to enable logging
  -n, --namespace=<namespace>
                         Specifies a Kubernetes namespace or OpenShift project,
                           or uses the current namespace if not specified
      --name=<name>      Name of the resource which should be converted (can be
                           used onl with --namespace and single --kind options)
```

### Required access rights

To convert the resources directly in your Kubernetes cluster, you must run the tool as a user with RBAC permission to:

* Get the Strimzi custom resources you are going to convert (when using the `--name` option)
* List the Strimzi custom resources you are going to convert (when not using the `--name` option)
* Replace the Strimzi custom resources you are going to convert

## Upgrading CRDs to v1

You can also use the tool to update the Strimzi CRDs to use the new `v1` version as a stored version.
This is required before upgrading to Strimzi 0.52 / 1.0.0 and newer.
You have to first convert all Strimzi custom resources to `v1` using one of the previous commands.
Once the custom resources are ready, you can use the `crd-upgrade` subcommand to upgrade the CRDs.
The following example command shows how the tool is used:

```
# Upgrade the Strimzi CRDs to v1
> bin/api-conversion.sh crd-upgrade
```

**After you have upgraded the CRDs to use `v1` as storage version, you must only use the fields available in `v1` in your custom resources.**

### Required access rights

To upgrade the Strimzi CRDs to `v1`, you must run the tool as a user with RBAC permission to:

* List the Strimzi custom resources in all namespaces
* Replace the Strimzi custom resources in all namespaces
* Patch the CRD resources
* Replace the status of the CRD resources

## List of changes to the Strimzi custom resources in the `v1` API version

### `Kafka` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually. 

* The `.spec` section is required and have to be present.
* `type: oauth` authentication is not supported in the `v1` API.
  Use `type: custom` authentication instead.
* The `secrets` property in `type: custom` authentication is not supported in the `v1` API.
  Use the `template` section instead to mount custom Secrets.
* The `type: keycloak` and `type: opa` authorizations are not supported anymore.
  Use `type: custom` authorization instead.
* The `.spec.kafka.resources` property is not supported in the `v1` API.
  Configure resource requests and limits in your `KafkaNodePool` resources.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* Change the `apiVersion` to `kafka.strimzi.io/v1`.
* Delete the `.spec.zookeeper` section.
  ZooKeeper-based Apache Kafka clusters are not supported anymore.
* Delete the `.spec.jmxTrans` section.
  JMX Trans is not supported anymore.
* Delete the following Kafka properties which are nto supported anymore:
  * `.spec.kafka.replicas` (replicas are configured through the `KafkaNodePool` resources)
  * `.spec.kafka.storage` (storage is configured through the `KafkaNodePool` resources)
  * `.spec.kafka.template.statefulset` 
* Delete the following Cruise Control properties which are not supported anymore:
  * `.spec.cruiseControl.tlsSidecar`
  * `.spec.cruiseControl.template.tlsSidecarContainer`
  * `.spec.cruiseControl.brokerCapacity.disk`
  * `.spec.cruiseControl.brokerCapacity.cpuUtilization`
* Delete the `.spec.kafkaExporter.template.service` property.
* Delete the following Entity Operator properties which are not supported anymore:
  * `.spec.entityOperator.tlsSidecar`
  * `.spec.entityOperator.template.tlsSidecarContainer`
  * `.spec.entityOperator.topicOperator.topicMetadataMaxAttempts`
  * `.spec.entityOperator.topicOperator.zookeeperSessionTimeoutSeconds`
  * `.spec.entityOperator.userOperator.zookeeperSessionTimeoutSeconds`
* Delete the `.spec.entityOperator.topicOperator.reconciliationIntervalSeconds` property and use the `.spec.entityOperator.topicOperator.reconciliationIntervalMs` property instead.
  The value should be the original value of `reconciliationIntervalSeconds` times 1000.
* Delete the `.spec.entityOperator.userOperator.reconciliationIntervalSeconds` property and use the `.spec.entityOperator.userOperator.reconciliationIntervalMs` property instead.
  The value should be the original value of `reconciliationIntervalSeconds` times 1000.
* In YAML files that include the status section, remove the `.status.registeredNodeIds`, `.status.kafkaMetadataState`, and `.status.listeners[].type` fields.
  This conversion is not needed for any resources that exist in your Kubernetes cluster and are managed by Strimzi Cluster Operator.

### `KafkaConnect` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.
* `type: oauth` authentication is not supported in the `v1` API.
  Use `type: custom` authentication instead.
* `.spec.externalConfiguration` is not supported in the `v1` API.
  Use the `.spec.template` section instead to add custom volumes or environment variables.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* The `.spec.replicas` field is required in the `v1` API.
  If you do not have it in your `KafkaConnect` custom resource, add it and set it to `3`.
* The `.spec.build.output.additionalKanikoOptions` property was renamed to `.spec.build.output.additionalBuildOptions` in the `v1` API.
* Delete the `.spec.template.deployment` property.
* `type: jaeger` tracing is not supported and should be removed.
* The `.spec.groupId`, `.spec.configStorageTopic`, `.spec.offsetStorageTopic`, and `.spec.statusStorageTopic` properties are required in the `v1` API.
  Add them and set them to the following values from `.spec.config` or to the default value.
  If needed, remove the value from `.spec.config` afterward.
  * For `.spec.groupId` use the `group.id` field from `.spec.config` or set it to `connect-cluster`.
  * For `.spec.configStorageTopic` use the `config.storage.topic` field from `.spec.config` or set it to `connect-cluster-configs`.
  * For `.spec.offsetStorageTopic` use the `offset.storage.topic` field from `.spec.config` or set it to `connect-cluster-offsets`.
  * For `.spec.statusStorageTopic` use the `status.storage.topic` field from `.spec.config` or set it to `connect-cluster-status`.

### `KafkaMirrorMaker2` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.
* `type: oauth` source or target Kafka cluster authentication is not supported in the `v1` API.
  Use `type: custom` authentication instead.
* `.spec.externalConfiguration` is not supported in the `v1` API.
  Use the `.spec.template` section instead to add custom volumes or environment variables.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* The `.spec.replicas` field is required in the `v1` API.
* Delete the `.spec.template.deployment` property.
* `type: jaeger` tracing is not supported in the `v1` API and should be removed.
* Heartbeat connector (`.spec.mirrors[].heartbeatConnector`) is not supported in the `v1` API and should be removed.
* The `pause` property (configured in connectors in `.spec.mirrors`) is not supported in the `v1` API and should be removed.
  If you have paused connectors (the `pause` property set to `true`), change it to `state: paused`.
* `topicBlacklistPattern` and `groupBlacklistPattern` properties (in `.spec.mirrors`) are not supported in the `v1` API and should be deleted.
  Use the `topicsExcludePattern` and `groupsExcludePattern` properties instead.
* The `.spec.connectCluster`, `.spec.clusters`, `.spec.mirrors[].sourceCluster`, and `.spec.mirrors[].targetCluster` properties are not supported in the `v1` API and should be removed.
  To replace them, configure the target (and Connect) Kafka cluster in `.spec.target` and the source cluster(s) in `.spec.mirrors[].source`.
  In `.spec.target`, set the `.spec.target.groupId`, `.spec.target.configStorageTopic`, `.spec.target.offsetStorageTopic`, and `.spec.target.statusStorageTopic` properties.¨
  * For `.spec.target.groupId` use the `group.id` field from `.spec.config` or set it to `mirrormaker2-cluster`.
  * For `.spec.target.configStorageTopic` use the `config.storage.topic` field from the `config` section of the target cluster or set it to `mirrormaker2-cluster-configs`.
  * For `.spec.target.offsetStorageTopic` use the `offset.storage.topic` field from the `config` section of the target cluster or set it to `mirrormaker2-cluster-offsets`.
  * For `.spec.target.statusStorageTopic` use the `status.storage.topic` field from the `config` section of the target cluster or set it to `mirrormaker2-cluster-status`.
  * Remove the value from the `config` section of the target cluster afterward.

### `KafkaBridge` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.
* `type: oauth` authentication is not supported in the `v1` API.
  Use `type: custom` authentication instead.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* The `.spec.replicas` field is required in the `v1` API.
  If you do not have it in your `KafkaBridge` custom resource, add it and set it to `1`.
* `type: jaeger` tracing is not supported and should be removed.
* The `.spec.enableMetrics` property is not supported in the `v1` API and should be removed.
  If you have it set to `true`, please use `.spec.metricsConfig` instead.
  The automatic conversion of the `enableMetrics` property will automatically create a new Config Map with the metric configuration.

### `KafkaConnector` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* The `.spec.pause` property is not supported in the `v1` API and should be removed.
  If you have paused connector (the `pause` property set to `true`), change it to `state: paused`.

### `KafkaRebalance` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.

### `KafkaNodePool` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* The `overrides` property in storage configuration (`.spec.storage`) is not supported in the `v1` API and should be removed.
  Use multiple different `KafkaNodePool` resources instead if you need different storage configuration for different nodes.

### `KafkaTopic` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.

### `KafkaUser` CR

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.

#### Supporting automatic conversion

The following changes can be done manually or using the conversion tool.

* The `operation` field in `.spec.authorization.acls[]` is not supported in the `v1` API and should be removed
  Use the `operations` array instead.
  **This conversion needs to be done BEFORE upgrading to Strimzi 0.49-0.51.**

### `StrimziPodSet` CR

The `StrimziPodSet` is internal Strimzi resource and should not be used directly by any Strimzi users.
You should not need to worry about converting it.
The following changes are listed _just-in-case_.

#### Requiring manual conversion

The following changes cannot be done by the conversion tool and have to be applied manually.

* The `.spec` section is required and have to be present.
