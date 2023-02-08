/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the Topic Operator deployment
 */
public class EntityTopicOperator extends AbstractModel {
    protected static final String COMPONENT_TYPE = "entity-topic-operator";

    protected static final String TOPIC_OPERATOR_CONTAINER_NAME = "topic-operator";
    private static final String NAME_SUFFIX = "-entity-topic-operator";
    private static final String CERT_SECRET_KEY_NAME = "entity-operator";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Topic Operator configuration keys
    /* test */ static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    /* test */ static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    /* test */ static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    /* test */ static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    /* test */ static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    /* test */ static final String ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    /* test */ static final String ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    /* test */ static final String ENV_VAR_SECURITY_PROTOCOL = "STRIMZI_SECURITY_PROTOCOL";

    /* test */ static final String ENV_VAR_TLS_ENABLED = "STRIMZI_TLS_ENABLED";

    private static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withInitialDelaySeconds(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT).build();

    // Volume name of the temporary volume used by the TO container
    // Because the container shares the pod with other containers, it needs to have unique name
    /* test */ static final String TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-to-tmp";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    /* test */ String kafkaBootstrapServers;
    /* test */ String zookeeperConnect;

    private String watchedNamespace;
    /* test */ int reconciliationIntervalMs;
    /* test */ int zookeeperSessionTimeoutMs;
    /* test */ String resourceLabels;
    /* test */ int topicMetadataMaxAttempts;
    private ResourceTemplate templateRoleBinding;

    private io.strimzi.api.kafka.model.Probe startupProbeOptions;

    /**
     * @param reconciliation   The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected EntityTopicOperator(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, resource.getMetadata().getName() + NAME_SUFFIX, COMPONENT_TYPE);

        this.readinessPath = "/";
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.livenessPath = "/";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;

        // new KafkaStreamsTopicStore needs a bit more to start
        this.startupProbeOptions = new ProbeBuilder()
                .withPeriodSeconds(10)
                .withFailureThreshold(12)
                .build();

        // create a default configuration
        this.kafkaBootstrapServers = KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT;
        this.zookeeperConnect = "localhost:" + EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT;
        this.reconciliationIntervalMs = EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);
        this.topicMetadataMaxAttempts = EntityTopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;

        this.ancillaryConfigMapName = KafkaResources.entityTopicOperatorLoggingConfigMapName(cluster);
        this.logAndMetricsConfigVolumeName = "entity-topic-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/topic-operator/custom-config/";
    }

    /**
     * Create an Entity Topic Operator from given desired resource. When Topic Operator (Or Entity Operator) are not
     * enabled, it returns null.
     *
     * @param reconciliation The reconciliation
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Topic Operator one
     *
     * @return Entity Topic Operator instance, null if not configured
     */
    public static EntityTopicOperator fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly) {
        if (kafkaAssembly.getSpec().getEntityOperator() != null
                && kafkaAssembly.getSpec().getEntityOperator().getTopicOperator() != null) {
            EntityTopicOperatorSpec topicOperatorSpec = kafkaAssembly.getSpec().getEntityOperator().getTopicOperator();
            EntityTopicOperator result = new EntityTopicOperator(reconciliation, kafkaAssembly);

            String image = topicOperatorSpec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE, "quay.io/strimzi/operator:latest");
            }
            result.image = image;
            result.watchedNamespace = topicOperatorSpec.getWatchedNamespace() != null ? topicOperatorSpec.getWatchedNamespace() : kafkaAssembly.getMetadata().getNamespace();
            result.reconciliationIntervalMs = topicOperatorSpec.getReconciliationIntervalSeconds() * 1_000;
            result.zookeeperSessionTimeoutMs = topicOperatorSpec.getZookeeperSessionTimeoutSeconds() * 1_000;
            result.topicMetadataMaxAttempts = topicOperatorSpec.getTopicMetadataMaxAttempts();
            result.logging = topicOperatorSpec.getLogging();
            result.gcLoggingEnabled = topicOperatorSpec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : topicOperatorSpec.getJvmOptions().isGcLoggingEnabled();
            result.jvmOptions = topicOperatorSpec.getJvmOptions();
            result.resources = topicOperatorSpec.getResources();
            if (topicOperatorSpec.getStartupProbe() != null) {
                result.startupProbeOptions = topicOperatorSpec.getStartupProbe();
            }
            if (topicOperatorSpec.getReadinessProbe() != null) {
                result.readinessProbeOptions = topicOperatorSpec.getReadinessProbe();
            }
            if (topicOperatorSpec.getLivenessProbe() != null) {
                result.livenessProbeOptions = topicOperatorSpec.getLivenessProbe();
            }

            if (kafkaAssembly.getSpec().getEntityOperator().getTemplate() != null)  {
                result.templateRoleBinding = kafkaAssembly.getSpec().getEntityOperator().getTemplate().getTopicOperatorRoleBinding();
            }

            return result;
        } else {
            return null;
        }
    }

    protected Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                TOPIC_OPERATOR_CONTAINER_NAME,
                image,
                List.of("/opt/strimzi/bin/topic_operator_run.sh"),
                securityProvider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                List.of(ContainerUtils.createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT)),
                getVolumeMounts(),
                ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath + "healthy", HEALTHCHECK_PORT_NAME),
                ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath + "ready", HEALTHCHECK_PORT_NAME),
                ProbeGenerator.httpProbe(startupProbeOptions, livenessPath + "healthy", HEALTHCHECK_PORT_NAME),
                imagePullPolicy,
                null
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Integer.toString(zookeeperSessionTimeoutMs)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_SECURITY_PROTOCOL, EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        ModelUtils.javaOptions(varList, jvmOptions);

        // Add shared environment variables used for all containers
        varList.addAll(ContainerUtils.requiredEnvVars());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    protected List<Volume> getVolumes() {
        return List.of(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
    }

    private List<VolumeMount> getVolumeMounts() {
        return List.of(VolumeUtils.createTempDirVolumeMount(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath),
                VolumeUtils.createVolumeMount(EntityOperator.ETO_CERTS_VOLUME_NAME, EntityOperator.ETO_CERTS_VOLUME_MOUNT),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
    }

    /**
     * Generates the Topic Operator Role Binding
     *
     * @param namespace         Namespace where the Topic Operator is deployed
     * @param watchedNamespace  Namespace which the Topic Operator is watching
     *
     * @return  Role Binding for the Topic Operator
     */
    public RoleBinding generateRoleBindingForRole(String namespace, String watchedNamespace) {
        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(KafkaResources.entityOperatorDeploymentName(cluster))
                .withNamespace(namespace)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName(KafkaResources.entityOperatorDeploymentName(cluster))
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        RoleBinding rb = RbacUtils
                .createRoleBinding(KafkaResources.entityTopicOperatorRoleBinding(cluster), watchedNamespace, roleRef, List.of(subject), labels, ownerReference, templateRoleBinding);

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(watchedNamespace)) {
            rb.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return rb;
    }

    /**
     * Generate the Secret containing the Entity Topic Operator certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Entity Topic Operator private key.
     *
     * Note: This certificate will be used by both Topic Operator Container and the TLS sidecar container. The User Operator Container use a separate certificate.
     *
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret.
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Secret secret = clusterCa.entityTopicOperatorSecret();
        return ModelUtils.buildSecret(reconciliation, clusterCa, secret, namespace, KafkaResources.entityTopicOperatorSecretName(cluster), componentName,
            CERT_SECRET_KEY_NAME, labels, ownerReference, isMaintenanceTimeWindowsSatisfied);
    }

    /**
     * @return Returns the namespace watched by the Topic Operator
     */
    public String watchedNamespace() {
        return watchedNamespace;
    }

    @Override
    public String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }
}
