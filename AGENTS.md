# Strimzi Kafka Operator

Strimzi provides a way to run Apache Kafka® clusters on Kubernetes and OpenShift using the Operator pattern.
It simplifies the deployment and management of Kafka clusters, including topics, users, connectors, and more.

See [README.md](README.md) for project overview, quick starts, and community information.

## Architecture

Multi-module Maven project.

- Java runtime, Kafka client and Fabric8 Kubernetes client versions: See `pom.xml`
- Supported Kafka versions (for cluster deployment): See `kafka-versions.yaml`

### Modules

| Module | Purpose                                                            |
|--------|--------------------------------------------------------------------|
| `cluster-operator/` | Main operator managing Kafka clusters                              |
| `topic-operator/` | Manages Kafka topics via KafkaTopic CRs                            |
| `user-operator/` | Manages Kafka users and ACLs via KafkaUser CRs                     |
| `api/` | CRD definitions and API models (Kafka, KafkaTopic, KafkaUser, etc.) |
| `operator-common/` | Shared operator code and utilities                                 |
| `kafka-agent/` | Java agent running inside Kafka containers (broker config, metrics) |
| `tracing-agent/` | OpenTelemetry tracing support                                      |
| `certificate-manager/` | TLS certificate management and CA handling                         |
| `kafka-init/` | Kafka container initialization logic                               |
| `crd-generator/` | Generates CRDs from `api` module via Java annotations              |
| `crd-annotations/` | Annotations for CRD generation                                     |
| `config-model/` | Kafka configuration models utility classes                         |
| `config-model-generator/` | Generates Kafka configuration models                               |
| `v1-api-conversion/` | Conversion tool from v1beta2 to v1 CRDs                    |
| `mockkube/` | Mock Kubernetes for unit testing                                   |
| `test/` | Test utilities and helpers                                         |
| `systemtest/` | End-to-end system tests                                            |

## Contributing

- **Commit sign-off (DCO)**: Required on all commits. See [CONTRIBUTING.md](CONTRIBUTING.md)
- **Code style**: Enforced by checkstyle. Config: `.checkstyle/checkstyle.xml`
- **Development setup**: See [DEV_GUIDE.md](development-docs/DEV_GUIDE.md)

## Developer Notes

### Common Development Tasks
- **CRD Changes**: After modifying `api/` module, need to build to regenerate CRDs in:
   - `api/src/test/resources/crds/`
   - `packaging/install/cluster-operator/`
   - `packaging/helm-charts/helm3/strimzi-kafka-operator/crds/`
- **Kafka Version Updates**: See [KAFKA_VERSIONS.md](development-docs/KAFKA_VERSIONS.md)
- **Generated Code** (Never edit directly):
   - Sundrio builders/fluent classes (from annotations)
   - Config Model JSONs (from Kafka source)
   - Files in `target/` or marked `@Generated`
- **Container Images**: Built via `docker-images/` Makefiles, based on `docker-images/base/`
- **Debugging**: See [DEBUGGING.md](development-docs/DEBUGGING.md) for remote debugging setup

## Additional Developer Resources

- **Debugging**: [DEBUGGING.md](development-docs/DEBUGGING.md) - Remote debugging operators and Kafka brokers
- **Kafka Versions**: [KAFKA_VERSIONS.md](development-docs/KAFKA_VERSIONS.md) - Adding/removing Kafka versions
- **Release Process**: [RELEASE.md](development-docs/RELEASE.md) - Release checklist and procedures
- **Governance**: [GOVERNANCE.md](GOVERNANCE.md) - Project governance structure
- **Version Support**: [KAFKA_VERSION_SUPPORT.md](KAFKA_VERSION_SUPPORT.md) - Kafka version support policy

## Build & Test

- **Building**: See [DEV_GUIDE.md](development-docs/DEV_GUIDE.md) for build prerequisites, commands, and options.
- **Testing**: See [TESTING.md](development-docs/TESTING.md) for comprehensive testing guide.

## Documentation

User-facing documentation is in `documentation/` folder (AsciiDoc format):
- **Overview guide** - Key concepts and features
- **Deploying and Managing guide** - Deployment instructions and best practices
- **API Reference** - Detailed configuration reference

Published documentation available at [strimzi.io/documentation](https://strimzi.io/documentation/).

For contributing to documentation, see the documentation related [README.md](documentation/README.md) and the [Documentation Contributor Guide](https://strimzi.io/contributing/guide/).