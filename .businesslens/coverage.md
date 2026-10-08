---
scope: The Cluster, Topic and User Operators, the custom resource API they reconcile, and the shared operator code.
method: Static reading of source, CRD model classes and user documentation; no code was built or run.
covered:
  - description: Custom resource API model classes
    paths:
      - api/src/main/java/io/strimzi/api/
  - description: Cluster Operator reconcilers, rolling updates and Cruise Control integration
    paths:
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/
  - description: Cluster Operator resource models, storage, listener and node pool validation
    paths:
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/model/
  - description: Cluster Operator periodic reconciliation
    paths:
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/ClusterOperator.java
  - description: Topic Operator
    paths:
      - topic-operator/src/main/
  - description: User Operator
    paths:
      - user-operator/src/main/
  - description: Shared operator code for annotations, certificate authorities and reconciliation
    paths:
      - operator-common/src/main/java/io/strimzi/operator/common/
      - certificate-issuer/src/main/
exclusions:
  - description: Unit, integration and system tests and their helpers
    paths:
      - systemtest/
      - test/
      - mockkube/
      - cluster-operator/src/test/
      - topic-operator/src/test/
      - user-operator/src/test/
      - operator-common/src/test/
      - api/src/test/
      - certificate-issuer/src/test/
  - description: Build, CRD and configuration-model generators and code-quality configuration
    paths:
      - crd-generator/
      - crd-annotations/
      - config-model-generator/
      - config-model/
      - .checkstyle/
      - .spotbugs/
      - tools/
  - description: CI pipelines and developer documentation
    paths:
      - .github/
      - .azure/
      - development-docs/
  - description: Container image builds
    paths:
      - docker-images/
  - description: Release copies of installation files, examples and Helm charts
    paths:
      - install/
      - examples/
      - helm-charts/
  - description: Example custom resources and dashboards
    paths:
      - packaging/examples/
unmapped:
  - description: Cluster Operator startup, operator configuration, feature gates, leader election and namespace watching
    paths:
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/Main.java
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/ClusterOperatorConfig.java
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/leaderelection/
      - operator-common/src/main/java/io/strimzi/operator/common/featuregates/
  - description: Standalone Topic and User Operator configuration
    paths:
      - topic-operator/src/main/java/io/strimzi/operator/topic/TopicOperatorConfig.java
      - user-operator/src/main/java/io/strimzi/operator/user/UserOperatorConfig.java
  - description: Internal cluster security settings of a Kafka cluster
    paths:
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/model/clustersecurity/
      - api/src/main/java/io/strimzi/api/kafka/model/kafka/clustersecurity/
  - description: Gatekeeper plugin API and its invokers
    paths:
      - api/src/main/java/io/strimzi/plugin/gatekeeper/
      - operator-common/src/main/java/io/strimzi/operator/common/gatekeeper/
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/gatekeeper/
      - user-operator/src/main/java/io/strimzi/operator/user/gatekeeper/
  - description: Pod security profile plugins
    paths:
      - api/src/main/java/io/strimzi/plugin/security/
  - description: Kubernetes restart events published by the Cluster Operator
    paths:
      - cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/resource/events/
  - description: Operator metrics
    paths:
      - operator-common/src/main/java/io/strimzi/operator/common/metrics/
  - description: Broker agent, init container and tracing agent
    paths:
      - kafka-agent/
      - kafka-init/
      - tracing-agent/
  - description: Operator installation files, RBAC roles, Drain Cleaner manifests and Helm chart
    paths:
      - packaging/install/
      - packaging/helm-charts/
limitations: []
---

# Coverage
