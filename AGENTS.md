# Strimzi Kafka Operator

Strimzi provides a way to run Apache Kafka® clusters on Kubernetes and OpenShift using the Operator pattern.
It simplifies the deployment and management of Kafka clusters, including topics, users, connectors, and more.

See [README.md](README.md) for project overview, quick starts, and community information.

## Architecture

Multi-module Maven project.

### Requirements

- **Java**: Java 21
- **Build tools**: Maven 3.5+, make, bash
- **Helm**: Helm 3 (for Helm chart packaging and testing)
- **Container runtime**: Docker or Podman (use `DOCKER_CMD=podman` for Podman)
- **Kubernetes cluster**: Required for integration/system tests (minikube, kind, or remote cluster)

### Key Dependencies

- Kafka client and Fabric8 Kubernetes client versions: See `pom.xml`
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

- **Commit sign-off (DCO)** (REQUIRED on all commits): always use `git commit -s`
  - If forgotten: `git commit --amend -s` to fix last commit
  - CI will fail without sign-off
  - More info: [CONTRIBUTING.md](CONTRIBUTING.md)

- **Code style**: Enforced by checkstyle (runs in CI on all PRs)
  - Config: `.checkstyle/checkstyle.xml`
  - Run locally: `mvn checkstyle:check`
  - CI will fail if checkstyle errors are found

## Developer Notes

### Common Development Tasks

**Building:**
- Build project: `make all` (compiles Java, runs tests, builds Docker images)
- Clean build artifacts: `make clean`
- Customize Maven behavior: Use `MVN_ARGS` environment variable
  - Skip all tests: `MVN_ARGS=-DskipTests make all`
  - Skip integration tests only: `MVN_ARGS=-DskipITs make all`
- Build specific module: Run `make all` or `make clean` from module directory (`MVN_ARGS` works here too)

**Testing:**
- Unit tests only: `mvn test`
- Unit + integration tests: `mvn verify`
- System tests (requires Kubernetes cluster): see [TESTING.md](development-docs/TESTING.md)

**CRD Changes:** If you changed anything in the `api/` module:
1. Run `make all` in the `api/` module to update generated files
2. Run `make crd_install` to update derived CRDs in:
   - `api/src/test/resources/crds/`
   - `packaging/install/cluster-operator/`
   - `packaging/helm-charts/helm3/strimzi-kafka-operator/crds/`

**Generated Code** (Never edit directly):
- Sundrio builders/fluent classes (generated from annotations)
- Config Model JSONs (generated from Kafka source)
- Files in `target/` directories or marked `@Generated`

**Container Images:**
- Built via `docker-images/` Makefiles
- Base images in `docker-images/base/`
- Use `DOCKER_CMD=podman` to use Podman instead of Docker
- Environment variables for custom registry:
  - `DOCKER_ORG`: your registry organization/username (e.g., Docker Hub or Quay.io username, default: `$USER`)
  - `DOCKER_REGISTRY`: registry to use (e.g., `docker.io`, `quay.io`, default: `docker.io`)
  - `DOCKER_TAG`: image tag (default: `latest`)
  - `DOCKER_ARCHITECTURE`: target architecture for container images (e.g., `amd64`, `arm64`)

## Documentation

User-facing documentation is in `documentation/` folder (AsciiDoc format):
- **Overview guide**: Key concepts and features
- **Deploying and Managing guide**: Deployment instructions and best practices
- **API Reference**: Detailed configuration reference

For contributing to documentation, see the documentation related [README.md](documentation/README.md).