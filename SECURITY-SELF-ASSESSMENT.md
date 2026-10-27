# Security Self-Assessment: Strimzi

## Table of Contents

* [Metadata](#metadata)
* [Overview](#overview)
  * [Background](#background)
  * [Actors](#actors)
  * [Isolation](#isolation)
  * [Actions](#actions)
  * [Goals](#goals)
  * [Non-Goals](#non-goals)
* [Self-Assessment Use](#self-assessment-use)
* [Security Functions and Features](#security-functions-and-features)
* [Project Compliance](#project-compliance)
* [Secure Development Practices](#secure-development-practices)
* [Security Issue Resolution](#security-issue-resolution)
* [Appendix](#appendix)
  * [Known Issues Over Time](#known-issues-over-time)
  * [OpenSSF Best Practices](#openssf-best-practices)
  * [OpenSSF Scorecard](#openssf-scorecard)
  * [Case Studies](#case-studies)
  * [Related Projects / Vendors](#related-projects--vendors)

## Metadata

|                       |                                                                                                                                                                                                                                                                                                                                                                                                  |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Assessment Stage      | Complete                                                                                                                                                                                                                                                                                                                                                                                         |
| Assessment Date       | 2026-10-08                                                                                                                                                                                                                                                                                                                                                                                       |
| Software              | [Strimzi](https://github.com/strimzi/strimzi-kafka-operator)                                                                                                                                                                                                                                                                                                                                     |
| Assessment Basis      | Strimzi `release-1.3.x` branch                                                                                                                                                                                                                                                                                                                                                                   |
| Target Release        | Strimzi 1.3.0                                                                                                                                                                                                                                                                                                                                                                                    |
| Release Status        | Released                                                                                                                                                                                                                                                                                                                                                                                         |
| Kubernetes Versions   | 1.32+ (as supported by the assessed release)                                                                                                                                                                                                                                                                                                                                                     |
| Apache Kafka Versions | 4.2.0, 4.2.1, 4.2.2, 4.3.0, 4.3.1 (default)                                                                                                                                                                                                                                                                                                                                                      |
| Website               | https://strimzi.io                                                                                                                                                                                                                                                                                                                                                                               |
| Security Provider     | No — Strimzi manages Apache Kafka on Kubernetes; it is not a security product                                                                                                                                                                                                                                                                                                                    |
| Languages             | Java 21                                                                                                                                                                                                                                                                                                                                                                                          |
| SBOM                  | Published per release via Syft, attached as attestations to container images using Sigstore                                                                                                                                                                                                                                                                                                      |
| Included Repositories | [strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator) (1.3.0), [drain-cleaner](https://github.com/strimzi/drain-cleaner) (1.6.1), [strimzi-kafka-bridge](https://github.com/strimzi/strimzi-kafka-bridge) (1.2.0), [strimzi-kafka-oauth](https://github.com/strimzi/strimzi-kafka-oauth) (0.18.0), [metrics-reporter](https://github.com/strimzi/metrics-reporter) (0.4.0) |
| Excluded from Scope   | Kubernetes control plane, CNI plugins, CSI drivers, external identity providers, third-party Kafka Connect plugins, client applications, cert-manager deployment                                                                                                                                                                                                                                 |

### Security Links

| Doc                        | URL                                                                                     |
|----------------------------|-----------------------------------------------------------------------------------------|
| Security Policy            | https://github.com/strimzi/.github/blob/main/SECURITY.md                                |
| Security Documentation     | https://strimzi.io/docs/operators/latest/overview#security-overview_str                 |
| OpenSSF Best Practices     | https://www.bestpractices.dev/en/projects/13184                                         |
| OpenSSF Scorecard          | https://scorecard.dev/viewer/?uri=github.com/strimzi/strimzi-kafka-operator             |
| GitHub Security Advisories | https://github.com/strimzi/strimzi-kafka-operator/security/advisories                   |
| Vulnerability Disclosure   | [cncf-strimzi-maintainers@lists.cncf.io](mailto:cncf-strimzi-maintainers@lists.cncf.io) |

## Overview

Strimzi is a CNCF incubating project that provides a Kubernetes-native way to run Apache Kafka clusters, Kafka Connect, Kafka MirrorMaker 2, and Kafka Bridge through the operator pattern.
It automates the deployment, configuration, management, and security of Apache Kafka on Kubernetes.

### Background

Apache Kafka is a distributed event streaming platform used by thousands of organizations for high-throughput, fault-tolerant data pipelines and streaming analytics.
Running Kafka on Kubernetes introduces specific security challenges: stateful workloads require persistent identity and stable networking, multi-port communication between brokers demands careful network policy management, and certificate management at scale becomes critical when every broker, controller, and client needs its own TLS certificate.
Communication between nodes within a Kafka cluster (controllers to controllers, brokers to controllers, brokers to brokers) adds an additional layer of complexity.
This makes some security designs more challenging than with other applications and has to be accounted for in implementation decisions.

Strimzi addresses these challenges through the Kubernetes operator pattern.
Operators extend the Kubernetes API with custom resources that describe the desired state of Kafka components, and reconciliation controllers continuously drive the actual state toward that desired state.
Strimzi's operators handle the operational complexity of running Kafka securely on Kubernetes, including automated TLS certificate generation and renewal, role-based access control delegation, and NetworkPolicy resource generation.

### Actors

Strimzi consists of operators and managed operands.
Operators may run as standalone Deployments (the Cluster Operator) or as multiple containers within a shared pod (the Topic Operator and User Operator run together within the Entity Operator pod).
Components use dedicated Kubernetes ServiceAccounts.
Security properties vary by interface: Kafka-protocol connections, Kubernetes API access, REST APIs, metrics endpoints, and health endpoints have independent security configurations.
Strimzi manages TLS certificates for internal Kafka-facing connections by default.
Both encryption and authentication for internal communication are configurable via the `strimzi.io/internal-cluster-security` annotation (see [SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)) and can be optionally disabled by users. 
Other interfaces have independent or user-configurable controls.
Prometheus metrics endpoints and health/readiness probes are served over plain HTTP without authentication. Access control for these endpoints is expected to be handled through Kubernetes NetworkPolicies or external mechanisms.

#### Cluster Operator

The Cluster Operator is the primary component of Strimzi.
It watches for `Kafka`, `KafkaNodePool`, `KafkaConnect`, `KafkaConnector`, `KafkaBridge`, `KafkaMirrorMaker2`, `KafkaRebalance`, and `StrimziPodSet` custom resources and reconciles the Kubernetes resources needed to run those components.
The Cluster Operator can be configured to watch specific namespaces or all namespaces.
Limiting the watched namespaces reduces the scope of cluster-wide resources the operator can manage, but does not eliminate all cluster-scoped roles — ClusterRoles and some ClusterRoleBindings are required for privilege delegation and for reading cluster-scoped resources such as Nodes.
During reconciliation, it manages certificates and security configuration for inter-component communication — by default generating TLS certificates (or creating cert-manager `Certificate` CRs via [SIP-100](https://github.com/strimzi/proposals/blob/main/100-external-certificate-manager.md)), but encryption and authentication can be reconfigured or disabled entirely via [SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md) — creates NetworkPolicy resources to restrict traffic between pods, and delegates RBAC to operand service accounts.
It uses seven Roles/ClusterRoles to implement least-privilege access control.

Kafka pods are managed through `StrimziPodSet` custom resources, a Strimzi-internal alternative to StatefulSets that provides finer control over pod lifecycle during rolling updates and scaling.
Reconciliation can be extended through the [Gatekeeper plugin system](https://github.com/strimzi/proposals/blob/main/144-Strimzi-Gatekeeper-plugin-system.md) (SIP-144), which supports validating and mutating plugins that can inspect or modify custom resources before and after reconciliation.

**Custom Resources:** `Kafka`, `KafkaNodePool`, `KafkaConnect`, `KafkaBridge`, `KafkaMirrorMaker2`, `KafkaRebalance`, `StrimziPodSet`

#### Topic Operator

The Topic Operator is an optional component that manages `KafkaTopic` custom resources, creating, updating, and deleting Kafka topics to match the desired state declared in Kubernetes.
It can be enabled and configured via the `Kafka` CR.
It can run in two modes: as a container within the Entity Operator pod alongside the User Operator when deployed as part of a Strimzi-managed Kafka cluster, or as a standalone deployment connecting to an external Kafka cluster not managed by Strimzi.
When deployed as part of a Strimzi-managed Kafka cluster, the security of the Kafka connection follows the cluster security configuration set via the `strimzi.io/internal-cluster-security` annotation: mTLS by default, but encryption and authentication are independently configurable and can each be disabled (see [SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)).
In standalone mode, communication is secured via mTLS using certificates provided by the user.
The Topic Operator operates within a single namespace and uses the Kafka Admin API to create, alter, and delete topics.

**Custom Resources:** `KafkaTopic`

#### User Operator

The User Operator is an optional component that manages `KafkaUser` custom resources, provisioning authentication credentials and authorization rules for Kafka clients.
It can be enabled and configured via the `Kafka` CR.
It creates TLS client certificates or SCRAM-SHA-512 passwords and stores them as Kubernetes Secrets in the same namespace as the KafkaUser resource.
The User Operator synchronizes ACL rules and quotas to Kafka, ensuring that the Kafka-side configuration matches the declared desired state.
Like the Topic Operator, it supports two deployment modes: within the Entity Operator pod as part of a Strimzi-managed Kafka cluster, or as a standalone deployment connecting to an external Kafka cluster not managed by Strimzi.
When deployed as part of a Strimzi-managed Kafka cluster, the security of the Kafka connection follows the cluster security configuration set via the `strimzi.io/internal-cluster-security` annotation: mTLS by default, but encryption and authentication are independently configurable and can each be disabled (see [SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)).
In standalone mode, it communicates with Kafka via mTLS.

**Custom Resources:** `KafkaUser`

#### Kafka Bridge

The Kafka Bridge provides an HTTP-based interface to Apache Kafka, enabling applications that cannot use the native Kafka protocol to produce and consume messages.
It runs as a separate Deployment managed by the Cluster Operator.
The Bridge exposes an inbound HTTP API that is a separate trust boundary with its own TLS configuration; TLS for inbound HTTP clients is supported but HTTP-level authentication for inbound clients is not provided by the Bridge itself and must be implemented externally (reverse proxy, API gateway, or network policy).
The outbound connection to the Kafka cluster supports TLS and configurable authentication.

**Custom Resources:** `KafkaBridge`

#### Kafka Connect connectors

Kafka Connect runs connector plugins that stream data between Kafka and external systems.
Strimzi manages Kafka Connect worker pods through an internal `StrimziPodSet`.
The Cluster Operator manages `KafkaConnector` custom resources that configure individual connector instances.
Kafka Connect exposes a REST API that can create and modify connectors and reveal sensitive configuration.
The generated NetworkPolicy restricts the REST API port (8083) to the Cluster Operator and Connect pods by default; it does not provide HTTP-level authentication.

Deployments should prefer `KafkaConnector` custom resources for Kubernetes-RBAC-mediated management and avoid external exposure of the REST API unless an authenticated proxy or equivalent control is used.
Connector plugins execute third-party code with the permissions and network access of the Connect workload and must be treated as trusted code.

The configurable internal cluster security ([SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)) applies only to the Kafka cluster itself (brokers, controllers, and internal operators).
It does not cover worker-to-worker communication between Kafka Connect pods or the internal Connect REST API (port 8083).
The REST API remains unauthenticated within its NetworkPolicy-enforced boundary.

Kafka Connect includes a build functionality that uses `Buildah` to create container images with additional connector plugins.
As of Strimzi 1.3.0, the `UseConnectBuildWithBuildah` feature gate has moved to GA stage and is the only supported build method — the previous Kaniko-based path has been removed.
Build pods require elevated privileges that are less strict than `RestrictedPodSecurityProvider`.
These can be configured within the `KafkaConnect` custom resource.

**Custom Resources:** `KafkaConnector`

#### Kafka Agent

The Kafka Agent is a Java agent embedded in the Kafka node JVM via the `-javaagent:` mechanism.
It runs within the same Kafka node container process — not as a separate sidecar container.
It exposes the Kafka node's readiness status and metadata over HTTPS on port 8443.
Only the Cluster Operator communicates with the Kafka Agent, and this communication is secured in the same way as the communication between the Kafka nodes.
The Agent enables the Cluster Operator to monitor Kafka node health and coordinate rolling updates without direct access to the Kafka protocol.

#### Cruise Control

Cruise Control is an automated Kafka cluster rebalancing engine that Strimzi integrates to optimize partition distribution across brokers.
It runs as a separate Deployment managed by the Cluster Operator.
Users interact with Cruise Control through `KafkaRebalance` custom resources, which trigger rebalancing proposals and approvals.
Communication between Cruise Control and the Cluster Operator is secured in the same way as the other internal communication within the Kafka cluster.

#### Kafka MirrorMaker 2

Kafka MirrorMaker 2 provides cross-cluster data replication for disaster recovery and geo-replication scenarios.
Strimzi deploys MirrorMaker 2 as a Kafka Connect cluster with pre-configured MirrorMaker connectors, and it inherits the Kafka Connect runtime and REST API trust boundaries.
Each source and target Kafka cluster connection is independently configured and may use TLS, mTLS, SCRAM-SHA-512, or other supported authentication modes.
The Cluster Operator manages the deployment through the `KafkaMirrorMaker2` custom resource.
As with Kafka Connect, the internal worker-to-worker communication and embedded REST API are not covered by SIP-150 configurable cluster security and remain protected only by NetworkPolicy.

**Custom Resources:** `KafkaMirrorMaker2`

#### Kafka Exporter

The Kafka Exporter (version 1.10.0 in Strimzi 1.3.0) uses Kafka APIs to read consumer-group and topic metadata and exposes the resulting metrics in Prometheus format.
It runs as a separate Deployment, connecting to the internal replication listener (port 9091) using mTLS.
When Kafka authorization is enabled, the Exporter's identity (`User:CN=<cluster>-kafka-exporter,O=io.strimzi`) is automatically added to `super.users` in the broker configuration, bypassing all ACLs.
This is necessary for the Exporter to read metadata without requiring per-topic ACL grants, but means a compromised Exporter pod has unrestricted access to the Kafka cluster.
The Exporter supplements the JMX metrics exposed natively by Kafka brokers with consumer-centric lag metrics critical for operational monitoring.
A new tool for additional Kafka metrics called Sova has been approved ([proposal #245](https://github.com/strimzi/proposals/pull/245)) and is targeted for future Strimzi releases.
Eventually this tool should replace Kafka Exporter.

#### Drain Cleaner

The [Drain Cleaner](https://github.com/strimzi/drain-cleaner) is a utility that ensures graceful Kafka pod evictions during Kubernetes node maintenance.
It runs as a Deployment with a `ValidatingWebhookConfiguration` that intercepts pod eviction requests.
When a Kafka pod is about to be evicted, the Drain Cleaner annotates the pod to trigger a controlled rolling restart by the Cluster Operator, preventing scenarios where topics become under-replicated during node draining.

### Isolation

Strimzi isolates its actors through multiple mechanisms.
RBAC is implemented through seven Roles/ClusterRoles for the Cluster Operator and its operands, following the principle of least privilege (additional ClusterRoles exist for the Drain Cleaner, and optional admin/view roles):

* `strimzi-cluster-operator-namespaced` — permissions for managing operand resources within watched namespaces
* `strimzi-cluster-operator-global` — permissions for managing cluster-scoped resources such as ClusterRoleBindings
* `strimzi-cluster-operator-leader-election` — permissions for leader election via Kubernetes Leases
* `strimzi-cluster-operator-watched` — permissions for watching custom resources across namespaces
* `strimzi-kafka-broker` — delegated to Kafka node pods for node awareness (rack-aware replica placement)
* `strimzi-entity-operator` — delegated to the Entity Operator for managing KafkaTopic and KafkaUser resources
* `strimzi-kafka-client` — delegated to Kafka client pods (Connect, MirrorMaker 2) for node awareness (rack-aware consumption)

Strimzi generates Kubernetes NetworkPolicy resources for managed components.
For Kafka listeners, the generated policies allow connections from all namespaces by default unless `networkPolicyPeers` is configured to restrict sources.
Other component interfaces have narrower defaults: the Kafka Connect REST API (port 8083) is restricted to the Cluster Operator and Connect pods; the Kafka Agent port (8443) is restricted to the Cluster Operator only.
Enforcement of NetworkPolicy resources depends on the Kubernetes cluster's CNI plugin and is outside Strimzi's control.

Kafka-protocol communication between Strimzi-managed components is configured with TLS and mTLS authentication by default, using certificates managed by two separate certificate authorities: the Cluster CA for component identity and the Clients CA for client authentication.
When Kafka authorization is enabled, Strimzi internal component identities (Cluster Operator, Entity Operator, Kafka Exporter, Cruise Control, and broker peers) are automatically granted `super.users` status in the broker configuration to allow internal operations without requiring explicit ACL grants.
Other interfaces (REST APIs, metrics endpoints, health endpoints) have independent security configurations.

Kafka brokers, Kafka Connect workers, and Kafka MirrorMaker 2 workers are managed through `StrimziPodSet` custom resources.
Cruise Control, Kafka Bridge, and Kafka Exporter run as standard Kubernetes `Deployment` resources.

### Actions

#### Kafka Cluster Deployment and Management

When a user creates or updates a `Kafka` custom resource, the Cluster Operator validates the resource and begins reconciliation.
It generates a Cluster CA and a Clients CA if they do not already exist, then creates per-broker TLS certificates signed by the Cluster CA.
The operator deploys Kafka brokers as a StrimziPodSet, configuring each broker with its TLS certificates, inter-broker authentication settings, and listener configurations.

It generates NetworkPolicy resources for each Kafka port; listener-level source restrictions require explicit `networkPolicyPeers` configuration.
RBAC resources are created for the broker, entity operator, and client service accounts using the delegation model.

To apply configuration changes, certificate renewals, and version upgrades, the Cluster Operator performs controlled rolling restarts with readiness and safety checks intended to reduce disruption.
Availability during rolling updates depends on cluster health, replication settings, PodDisruptionBudgets, Kubernetes scheduling capacity, and storage/network health.
If certificates have been renewed, the new certificates are distributed before the rolling restart begins.

#### Certificate Lifecycle Management

Strimzi automatically manages the lifecycle of TLS certificates used for internal communication.
The Cluster Operator monitors certificate expiration and initiates renewal before a certificate expires.
During renewal, the operator generates new certificates, maintains the old CA certificate alongside the new one during a configurable grace period, and performs a rolling restart of affected components to pick up the new certificates.
Users can also provide their own CA certificates through Kubernetes Secrets, in which case Strimzi manages only the end-entity certificates.

As an alternative, Strimzi supports delegating certificate issuance to [cert-manager](https://github.com/strimzi/proposals/blob/main/100-external-certificate-manager.md) (SIP-100).
When using cert-manager, Strimzi never holds the CA private keys — the user provides a cert-manager Issuer reference and the CA public certificate, and cert-manager handles issuance and renewal.
This enables integration with enterprise PKI systems and external certificate authorities.

#### User Credential Provisioning

When a `KafkaUser` resource is created, the User Operator provisions the requested credentials.
For TLS authentication, it generates a client certificate signed by the Clients CA and stores the certificate, private key, and CA certificate in a Kubernetes Secret.
For SCRAM-SHA-512 authentication, it generates a random password and stores it in a Secret.
The User Operator then configures the corresponding ACLs and quotas in Kafka.
All credential storage uses Kubernetes Secrets with the operator managing the full lifecycle, including renewal for TLS credentials.

The PKCS12 format for credential Secrets is deprecated as of Strimzi 1.3.0.
PEM will be the only supported format in a future releases.

#### Listener Security Configuration

TLS and authentication are configured independently for each Kafka listener.
Supported authentication mechanisms include mTLS, SCRAM-SHA-512, and custom authentication plugins (which can be used for OAuth 2.0 and other protocols).
Kafka authorization is configured at the cluster level through the broker authorizer, not independently per listener.
Strimzi supports the `simple` built-in authorization model and `custom` authorizer plugins, that allow users to use their own authorizer implementation.

The Cluster Operator generates listener-specific NetworkPolicy rules; source restrictions require explicit `networkPolicyPeers` configuration per listener.

### Goals

#### Generic Goals

* Provide a Kubernetes-native way to run Apache Kafka using the operator pattern
* Automate the deployment, configuration, and lifecycle management of Kafka clusters on Kubernetes
* Support multiple Kafka versions and provide upgrade paths between them
* Support namespace-scoped deployments with configurable operator watch scope and delegated RBAC
* Integrate with the broader cloud-native ecosystem including monitoring (Prometheus, Grafana), tracing (OpenTelemetry), packaging (Helm, OLM), authorization (OPA, Keycloak), and others.
* Provide extensive configurability through custom resources and extensibility through plugin interfaces such as custom authentication, authorization, pod security providers, and the [Gatekeeper plugin system](https://github.com/strimzi/proposals/blob/main/144-Strimzi-Gatekeeper-plugin-system.md) for validating and mutating resources during reconciliation

#### Security Goals

* Configure internal Kafka-protocol communication with TLS and mTLS authentication by default, with [configurable security modes](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md) to allow integration with service meshes or trade encryption for performance in isolated environments
* Automate the certificate lifecycle including CA generation, certificate issuance, renewal, and rotation — directly via the built-in CA or delegated to cert-manager (SIP-100)
* Implement least-privilege RBAC through dedicated ClusterRoles for each component
* Generate NetworkPolicy resources for managed components to support network segmentation
* Support configurable per-listener authentication and cluster-level Kafka authorization
* Run containers as non-root (UID 1001) by default in Strimzi-provided images
* Support operation on FIPS-enabled Kubernetes platforms
* Sign container images and publish SBOMs, provenance attestations for Strimzi container images, Helm OCI charts, and release artifacts; verification is not automatically enforced during deployment
* Scan dependencies and container images for vulnerabilities and licensing issues (Snyk + FOSSA)

### Non-Goals

#### General Non-Goals

* Strimzi does not provide a Kafka distribution; it uses the official Apache Kafka binaries
* Strimzi does not manage the underlying Kubernetes cluster or its infrastructure
* Strimzi does not manage Kafka client applications or their deployment
* Strimzi does not provide a managed Kafka service; it is an operator for self-managed deployments

#### Security Non-Goals

* Strimzi does not secure the underlying Kubernetes cluster or its API server
* Strimzi does not encrypt Kafka data at rest (log segments, topic data on disk); encryption of persistent storage is the responsibility of the Kubernetes storage provider or infrastructure layer
* Strimzi does not enable Kubernetes audit logging or define its retention policy
* Strimzi does not enable Kubernetes Secret encryption at rest
* Strimzi does not audit or inspect Kafka message content
* Strimzi does not guarantee the security of third-party Kafka Connect plugins, custom authentication modules, custom authorizers, or connector code deployed through the build functionality
* Strimzi does not replace the Kubernetes NetworkPolicy controller; it generates NetworkPolicy resources that require a CNI plugin to enforce
* Strimzi does not guarantee that generated NetworkPolicies are enforced by the cluster's CNI implementation
* Strimzi does not enforce image signature verification at admission; environments requiring enforcement should use admission policies
* Strimzi does not manage external access security beyond listener-level TLS and authentication configuration
* Strimzi does not secure external load balancers, ingress controllers, Routes, Gateway implementations, DNS, or upstream firewalls
* Strimzi does not guarantee client certificate reload or application-side credential rotation
* Strimzi does not enforce pod security standards on the Kubernetes cluster; the default pod security provider for managed operands is `BaselinePodSecurityProvider`; a `RestrictedPodSecurityProvider` is available and can be configured when the environment supports it (as of 1.2.0, the operator's own Deployment uses a restricted security context by default)
* Strimzi does not validate the security of custom Gatekeeper plugins; plugins execute as trusted code within the operator process
* Strimzi does not manage the cert-manager deployment or its Issuers; cert-manager availability and configuration are the cluster administrator's responsibility

## Self-Assessment Use

This document serves to provide Strimzi users with an initial understanding of Strimzi's security, where to find existing security documentation, and the general security posture of the project.
It also provides the CNCF TAG Security with an initial understanding of Strimzi as part of the joint-assessment process required for graduation.
Together, this document will inform and provide a basis for a formal security audit.

This self-assessment describes the security architecture, project practices, shared responsibilities, and known limitations of the assessed Strimzi release.
It is authored by the Strimzi project and is not an independent security audit, certification, or guarantee that every deployment is secure.
The security of a Strimzi deployment also depends on Kubernetes configuration, infrastructure, external integrations, user-selected security options, and operational practices.

This assessment is based on the versions of Strimzi and its components defined in the [Metadata](#metadata) table.
Dynamic content such as OpenSSF badge scores, Scorecard results, and advisory counts is point-in-time as of the assessment date.
Claims about CI/CD workflows, supported architectures, and approval requirements reflect project configuration at the time of writing and may change.

## Security Functions and Features

For additional context on how these features interact across components, see the [Actors](#actors) and [Actions](#actions) sections above.

### Critical

**Configurable internal communication security ([SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)) — Early Access in 1.3.0.**
Communication between Strimzi-managed Kafka cluster components (brokers, controllers, Cluster Operator, Entity Operator, Cruise Control, Kafka Exporter, and Kafka Agent) is configured with TLS encryption and mTLS authentication by default.
As of Strimzi 1.3.0 (Early Access - not recommended for production use), encryption (`tls` or `none`) and authentication (`mtls`, `service-account`, or `none`) can be configured independently via the `strimzi.io/internal-cluster-security` annotation to support service mesh integration or performance optimization in isolated environments.
This feature applies to Strimzi internal connections only. 
It does not extend to Kafka Connect or MirrorMaker 2 worker-to-worker communication.

The Kubernetes ServiceAccount authentication mode uses short-lived, audience-scoped OAUTHBEARER tokens (via [strimzi-kafka-oauth](https://github.com/strimzi/strimzi-kafka-oauth)) as an alternative to mTLS certificates.

Configuring internal communication without authentication requires the cluster to allow the resulting unauthenticated Kafka principal to perform internal operations.
Depending on the implementation, this may require broad permissions for the `ANONYMOUS` principal or disabling authorization for those connections.
Other interfaces (REST APIs, metrics endpoints, health probes) have independent security configurations.

**Automatic CA certificate generation and lifecycle management.**
Strimzi generates and manages two internal certificate authorities: the Cluster CA for component identity and the Clients CA for client authentication.
Certificate renewal, rotation, and the transition period during CA replacement are handled automatically by the Cluster Operator.
Users can also provide their own CA certificates.

As an alternative, Strimzi supports delegating certificate issuance to cert-manager ([SIP-100](https://github.com/strimzi/proposals/blob/main/100-external-certificate-manager.md)); in this mode, Strimzi never holds the CA private keys — the user provides a cert-manager Issuer reference and the CA public certificate.

**RBAC delegation model.**
The Cluster Operator uses ClusterRoles with namespace-scoped RoleBindings and, where required, ClusterRoleBindings.
The roles are separated and scoped by function to reduce unnecessary permissions and limit the impact of a compromised component.
See the [Isolation](#isolation) section for details on the seven ClusterRoles.

**Automatic NetworkPolicy generation.**
The Cluster Operator generates Kubernetes NetworkPolicy resources for each managed component.
For Kafka listeners, the generated policies allow connections from all namespaces by default; source restrictions require explicit `networkPolicyPeers` configuration.
NetworkPolicy generation is enabled by default and can be disabled if the cluster uses an alternative network security mechanism.
Enforcement depends on the Kubernetes cluster's CNI plugin.

**Non-root container execution.**
Strimzi-provided container images run as non-root with UID 1001 by default.
The default pod security provider for managed operands is `BaselinePodSecurityProvider`.
The `RestrictedPodSecurityProvider` is available for environments that support the restricted Kubernetes pod security profile; as of Strimzi 1.2.0, the operator's own Deployment uses a restricted security context in the installation files and Helm chart.
Template overrides in custom resources can modify these defaults.

### Security Relevant

**Per-listener authentication.**
Each Kafka listener can be configured with its own authentication mechanism: mTLS, SCRAM-SHA-512, OAuth 2.0, or custom authentication plugins.

**Cluster-level Kafka authorization.**
Kafka authorization is configured at the cluster level through the broker authorizer using the `simple` built-in model or `custom` authorizer plugins.

**RestrictedPodSecurityProvider.**
An optional pod security profile that sets `allowPrivilegeEscalation` to `false`, drops all Linux capabilities, enables the `RuntimeDefault` seccomp profile, and enforces `runAsNonRoot`.

Kafka Connect build security requirements depend on Kubernetes platform.
`Buildah` requires elevated privileges and security-context settings that are incompatible with the `RestrictedPodSecurityProvider`.
When the restricted profile is enabled, incompatible Kafka Connect build configurations are rejected rather than executed with elevated permissions.

**Kafka Connect and MirrorMaker 2 worker-to-worker security limitation.**
The configurable internal cluster security ([SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)) applies only to the Kafka cluster itself — Kafka brokers, controllers, and the internal operators that communicate with them.
It does not extend to communication between Kafka Connect worker pods or between MirrorMaker 2 worker pods.
Worker-to-worker traffic (used for task coordination and distributed mode offset management) is not encrypted or authenticated by Strimzi.
The internal Connect REST API (port 8083) likewise has no HTTP-level authentication; access is constrained only by the generated NetworkPolicy, which restricts port 8083 to the Cluster Operator and Connect pods.
Environments with strict lateral-movement requirements should layer an authenticated proxy, a service mesh, or Kubernetes NetworkPolicy with tighter peer selectors in front of Kafka Connect and MirrorMaker 2 worker pods.

**Custom CA certificates.**
Users can provide their own CA certificates through Kubernetes Secrets, allowing Strimzi to integrate with existing PKI infrastructure.

**Per-listener NetworkPolicy peer restrictions.**
Individual listeners can restrict inbound traffic to specific namespaces or pod selectors beyond the default NetworkPolicy rules.

**FIPS mode.**
Strimzi supports operation on Kubernetes platforms configured in FIPS mode.
When running on a FIPS-enabled host, the underlying Red Hat build of OpenJDK automatically activates FIPS-compliant cryptographic providers.
Strimzi propagates the `FIPS_MODE` environment variable to all managed pods so that this behaviour can be disabled when needed.
This does not constitute a product-level FIPS certification and depends on the cryptographic providers supplied by the underlying platform.

**Gatekeeper plugin system ([SIP-144](https://github.com/strimzi/proposals/blob/main/144-Strimzi-Gatekeeper-plugin-system.md)).**
An extensible plugin system that supports validating and mutating plugins invoked during reconciliation.
Mandatory plugins are hardcoded and cannot be disabled.
Default plugins ship with Strimzi but can be overridden, custom plugins can be added via the operator classpath.

Custom plugins execute third-party code within the operator process and have access to the Kubernetes API client — they must be treated as trusted code.
Mutating plugins can alter the effective custom-resource configuration used during reconciliation without persisting those mutations back to the Kubernetes API.
As a result, Kubernetes API audit logs record the submitted resource but not necessarily the effective post-plugin configuration.
Operators should use trusted plugins and retain appropriate Cluster Operator logs where an audit trail of plugin decisions is required.

**Kubernetes ServiceAccount authentication ([SIP-150](https://github.com/strimzi/proposals/blob/main/150-configurable-security-of-internal-communication.md)) — Early Access in 1.3.0.**
An alternative to mTLS for internal Kafka authentication using short-lived, audience-scoped Kubernetes ServiceAccount tokens via the OAUTHBEARER SASL mechanism.
Tokens are scoped to a specific Strimzi audience to prevent cross-cluster reuse.
This authentication mode is available as part of the SIP-150 Early Access feature in 1.3.0 and is not recommended for production use.

**cert-manager integration ([SIP-100](https://github.com/strimzi/proposals/blob/main/100-external-certificate-manager.md)) — Early Access in 1.3.0.**
Allows delegating end-entity certificate issuance to cert-manager instead of the built-in CA.
Each CA (Cluster CA, Clients CA) can independently use the built-in Strimzi CA or cert-manager, allowing gradual adoption.
When using cert-manager, Strimzi never holds CA private keys — the user provides a cert-manager Issuer reference and the CA public certificate, and cert-manager handles issuance and renewal.
This feature is available as Early Access in 1.3.0 and is not recommended for production use.

## Project Compliance

Strimzi does not currently hold formal compliance certifications such as PCI-DSS, ISO 27001, or SOC 2.
The project aligns with industry security practices in the following areas:

* **CNCF Supply Chain Security Best Practices**: Container images are signed using keyless signing with GitHub OIDC via Sigstore. SBOMs are generated using Syft and attached as attestations to container images. Build provenance attestations are generated for builds from `main` and from release branches.
* **OpenSSF Best Practices**: Strimzi holds a [passing badge at 100%](https://www.bestpractices.dev/en/projects/13184), achieved on 2026-06-12, covering all criteria across basics, change control, reporting, quality, security, and analysis.
* **OpenSSF Scorecard**: Actively monitored with results [published on scorecard.dev](https://scorecard.dev/viewer/?uri=github.com/strimzi/strimzi-kafka-operator). The scorecard assessment runs weekly via GitHub Actions.
* **Vulnerability Scanning**: Snyk scans Maven dependencies and container images on every push to `main`. The Snyk GitHub integration also evaluates pull requests and reports newly introduced dependency risks.
* **License Compliance**: FOSSA scans Maven dependencies and container images on every push to `main`. The FOSSA GitHub integration also evaluates pull requests for dependency and licensing changes.
* Both Snyk and FOSSA are also running during release workflow.

## Secure Development Practices

### Development Pipeline

Strimzi uses GitHub Actions for continuous integration and delivery.
The CI pipeline builds the Java project with Maven, runs unit tests with Surefire, integration tests with Failsafe against Kubernetes environment (Kind), and system tests against multi-node Kubernetes environments (Kind).
Container images are built for four architectures (`amd64`, `arm64`, `ppc64le`, `s390x`).

All third-party GitHub Actions are pinned by commit SHA to prevent supply chain attacks through compromised action versions.
Workflow permissions default to `contents: read` and are scoped per job to the minimum required permissions.

Static analysis is performed by CodeQL on every push to `main`, on pull requests, and on a weekly schedule.
Snyk and FOSSA workflows run for every push to `main` and during release workflows.
Their GitHub integrations also inspect pull requests and provide feedback before changes are merged.
Snyk results are published through the GitHub code scanning dashboard and their app, while FOSSA reports licensing and dependency-policy findings directly in their app.
A pre-commit workflow enforces code formatting and linting standards.

All pull requests require approval from at least two maintainers, or one maintainer and one component owner, before merging.

### Communication Channels

* **Internal**: CNCF Slack #strimzi-dev for developer coordination, private maintainer channel for security-sensitive topics, cncf-strimzi-maintainers mailing list, strimzi-dev mailing list
* **Inbound**: GitHub Issues for bug reports and feature requests, CNCF Slack #strimzi for user questions, strimzi-users mailing list, security vulnerabilities via [cncf-strimzi-maintainers@lists.cncf.io](mailto:cncf-strimzi-maintainers@lists.cncf.io)
* **Outbound**: GitHub Releases for release announcements, CNCF mailing list for community updates, [Strimzi blog](https://strimzi.io/blog/) for technical articles, GitHub Security Advisories for vulnerability disclosures

### Ecosystem

Strimzi operates within the broader cloud-native and Apache Kafka ecosystems.
It requires Kubernetes as its deployment platform and can be installed via YAML files, Helm charts, or the Operator Lifecycle Manager (OLM).
Strimzi integrates with a wide range of cloud-native projects: Prometheus and Grafana for monitoring, OpenTelemetry for tracing, Istio for service mesh networking, and cert-manager for external certificate management.
Strimzi supports custom authentication and authorization plugins, including integrations built around OAuth 2.0, OPA, Keycloak, and other external security services.
The project manages integrations with components from the Apache Kafka ecosystem including Kafka Connect for data integration, MirrorMaker 2 for cross-cluster replication, and Cruise Control for automated cluster rebalancing.

## Security Issue Resolution

### Responsible Disclosure Process

Security vulnerabilities should be reported privately to [cncf-strimzi-maintainers@lists.cncf.io](mailto:cncf-strimzi-maintainers@lists.cncf.io).
Public GitHub issues must not be used for security reports to avoid exposing active vulnerabilities before fixes are available.
The full security policy is maintained at the [organization level](https://github.com/strimzi/.github/blob/main/SECURITY.md).

### Vulnerability Response Process

Reported vulnerabilities are triaged by the Strimzi maintainers, who assess severity and develop patches.
Fixes are released as patch versions of the current minor release or included in the upcoming minor release, depending on severity.
For vulnerabilities in base container images, Strimzi maintains a dedicated [CVE rebuild workflow](https://github.com/strimzi/strimzi-kafka-operator/actions/workflows/cve-rebuild.yml) that rebuilds containers without modifying the Java application layer.
This workflow includes a manual approval gate that requires maintainer sign-off before publishing the rebuilt images.

Strimzi depends on Apache Kafka binaries from the upstream Apache Kafka project and uses additional external projects such as Cruise Control or Kafka Exporter.
Vulnerabilities in Kafka, Cruise Control, Kafka Exporter or its dependencies must be fixed and released by the respective project before Strimzi can adopt the fix.

### Incident Response

The incident response process follows a structured workflow: triage of the reported vulnerability, coordination among maintainers to develop and test a fix, publication of a patched release, creation of a GitHub Security Advisory with CVE assignment, and notification to the community through release notes and mailing list announcements.

## Appendix

### Known Issues Over Time

As of 2026-07-26, Strimzi has published five security advisories:

| CVE                                                                                                         | Severity | Description                                                                                                       | Affected Boundary                           |
|-------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| [CVE-2025-66623](https://github.com/strimzi/strimzi-kafka-operator/security/advisories/GHSA-xrhh-hx36-485q) | High     | Unrestricted access to all Secrets in the same Kubernetes namespace from Kafka Connect and MirrorMaker 2 operands | Operand Secret RBAC, namespace isolation    |
| [CVE-2026-27133](https://github.com/strimzi/strimzi-kafka-operator/security/advisories/GHSA-6x85-j2f7-4xc5) | Medium   | All CAs from CA chain trusted in Kafka Connect and Kafka MirrorMaker 2 target clusters                            | Certificate trust-anchor validation         |
| [CVE-2026-27134](https://github.com/strimzi/strimzi-kafka-operator/security/advisories/GHSA-2qwx-rq6j-8r6j) | High     | All CAs from a custom CA chain trusted for mTLS user authentication                                               | Certificate trust-anchor validation         |
| [CVE-2026-55225](https://github.com/strimzi/strimzi-kafka-operator/security/advisories/GHSA-mw9r-p8xp-wx96) | High     | Cross-namespace privilege escalation via `Kafka.spec.entityOperator`                                              | Namespace boundary, CR reference validation |
| [CVE-2026-55226](https://github.com/strimzi/strimzi-kafka-operator/security/advisories/GHSA-r427-j2h7-wv3m) | Medium   | Unrestricted access to all Secrets within namespace watched by the Topic Operator                                 | Operand Secret RBAC, least privilege        |

All advisories are listed on the [GitHub Security Advisories page](https://github.com/strimzi/strimzi-kafka-operator/security/advisories).

### OpenSSF Best Practices

Strimzi holds a passing OpenSSF Best Practices badge at 100% completion, covering all criteria across basics, change control, reporting, quality, security, and analysis.
The badge was achieved on 2026-06-12.

Badge: [bestpractices.dev/en/projects/13184](https://www.bestpractices.dev/en/projects/13184)

### OpenSSF Scorecard

The OpenSSF Scorecard runs weekly via GitHub Actions and results are published publicly.

Scorecard: [scorecard.dev](https://scorecard.dev/viewer/?uri=github.com/strimzi/strimzi-kafka-operator)

### Case Studies

**NuBank.**
Julio Turolla and Roni Silva from NuBank presented at [StrimziCon 2024](https://strimzi.io/blog/2024/06/06/strimzicon2024-roundup/) on modernizing NuBank's Kafka platform.
NuBank uses Strimzi to manage a Kafka deployment processing over one trillion messages per month, using a cell-based architecture to minimize the blast radius of failures and performing topic-by-topic live migration to Strimzi-managed clusters.

**Reddit.**
Sky Kistler from Reddit presented at [StrimziCon 2026](https://strimzi.io/blog/2026/06/23/strimzicon2026-roundup/) on migrating Reddit's Kafka fleet from EC2 to Kubernetes with zero downtime.
Reddit operates over 500 Kafka brokers managing petabytes of data on Strimzi, using a stretch-cluster approach with DNS facades for the migration.

**Randoli.**
Rajith Attapattu from Randoli presented at [StrimziCon 2026](https://strimzi.io/blog/2026/06/23/strimzicon2026-roundup/) a real-world case study of running Strimzi at scale in production, covering architecture decisions, monitoring, the upgrade to Strimzi 1.0.0, and building observability with OpenTelemetry.

### Related Projects / Vendors

Several vendors build commercial products on top of Strimzi:

* **Red Hat** — [Streams for Apache Kafka](https://developers.redhat.com/products/streams-for-apache-kafka/overview)
* **Cloudera** — [Stream Messaging](https://docs.cloudera.com/csm-operator/1.4/index.html)
* **Axual** — Kafka platform built on Strimzi
* **Ænix** — Kafka as a Service in [Cozystack](https://cozystack.io)

**Confluent Operator** is a commercial Kubernetes operator provided by Confluent for deploying Confluent Platform.
Unlike Strimzi, which manages open-source Apache Kafka, Confluent Operator is designed specifically for the commercial Confluent distribution and requires a Confluent subscription.

**Koperator** (formerly MSKope, by Banzai Cloud) was an open-source Kafka operator for Kubernetes.
The project has been archived and is no longer actively maintained.
