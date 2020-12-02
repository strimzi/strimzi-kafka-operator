# CRD conversion

With Strimzi's CRD evolving, there needs to be a way to easily migrate existing CR configuration to the new CRD schema.
For this purpose a custom Converter for each CRD needs to be implemented.

There are two methods to implement:

```
public void convertTo(T instance, ApiVersion toVersion)
public void convertTo(JsonNode node, ApiVersion toVersion)
```

The first one does conversion directly on the CR instance, which might later lead to reordered configuration output (think "git merge" problems).
That's why we added the second method, doing the conversion directly on the CR' JsonNode instance, preserving the configuration output as much as possible. 

The code already provides useful conversion methods, such as `move`, `delete`, `replace`. 
Hence you mostly just need to implement part of `replace` method; e.g. the actual replacement value when a CR is being changed in a non-trivial way (as was the case for Kafka.spec.kafka.listeners).

Current Converters are:
* [KafkaConverter](src/main/java/io/strimzi/kafka/crd/convert/converter/KafkaConverter.java)

### CRD conversion tooling / CLI

To simplify migration as much as possible, we added a new conversion CLI tool:

```
% crd --help                                                                                                                     
Usage: crd [-hV] [COMMAND]
Simple entry command
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help        Displays help information about the specified command
  convert, c  Convert CRDs
  
% crd c     
Missing required option: '--to-version=<toApiVersion>'
Usage: crd convert [-d] [-uo] [-fv=<fromApiVersion>] [-it=<inputType>]
                   [-ll=<level>] [-n=<namespace>] [-o=<outputFile>]
                   [-ot=<outputType>] -tv=<toApiVersion> [-f=<inputFile> |
                   -c=<content>] [<Kube API params>...]
Convert CRDs
  [<Kube API params>...]

  -c, --content=<content>               CRD content
  -d, --debug                           Use debug?
  -f, --file=<inputFile>                CRD input file
  -fv, --from-version=<fromApiVersion>  From K8s ApiVersion - required when used with Kube API
  -it, --input-type=<inputType>         Content input type, e.g. json or yaml
  -ll, --log-level=<level>              Set log level to enable logging
  -n, --namespace=<namespace>           K8s / OpenShift namespace
  -o, --output=<outputFile>             CRD output file
  -ot, --output-type=<outputType>       Content output type, e.g. json or yaml
  -tv, --to-version=<toApiVersion>      To K8s ApiVersion
  -uo, --openshift                      Use OpenShift client?
```

CRD conversion CLI uses [Picocli](https://picocli.info/) to implement its CLI commands.

At build time we create an uber-jar, holding all required dependencies.
This uber-jar acts as a sort of executable.

To ease the usage we suggest adding alias:
* `alias crd='java -cp "<PATH_TO_STRIMZI_PROJECT>/crd-convert/target/crd-convert-0.22.0-SNAPSHOT-jar-with-deps-cli.jar" io.strimzi.kafka.crd.convert.cli.EntryCommand'`

During the build you can also find completion script in target/ directory: `crd_completion.sh`.
You can `source` this script and get tab completion ootb.

Once you have things up-n-running, simply ask CLI for `help`. It will print out the usage, etc (see above).

Using the CLI - examples:
* crd c -tv v1beta2 -f=/strimzi-kafka-operator/crd-convert/src/test/resources/io/strimzi/kafka/crd/convert/converter/old-config.yaml
* crd c -tv v1beta2 -f=/strimzi-kafka-operator/crd-convert/src/test/resources/io/strimzi/kafka/crd/convert/converter/old-config.yaml -o /tmp/test.yaml
* crd c -fv v1beta1 -tv v1beta2 -n myproject kafka my-cluster 
