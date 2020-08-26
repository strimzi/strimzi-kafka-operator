/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.model.OrderedProperties;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.operator.cluster.model.ModelUtils.createHttpProbe;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Represents the User Operator deployment
 */
public class EntityUserOperator extends AbstractModel {
    protected static final String APPLICATION_NAME = "entity-user-operator";

    protected static final String USER_OPERATOR_CONTAINER_NAME = "user-operator";
    private static final String NAME_SUFFIX = "-entity-user-operator";
    protected static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8081;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // User Operator configuration keys
    public static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_LABELS";
    public static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME = "STRIMZI_CA_CERT_NAME";
    public static final String ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME = "STRIMZI_CA_KEY_NAME";
    public static final String ENV_VAR_CLIENTS_CA_NAMESPACE = "STRIMZI_CA_NAMESPACE";
    public static final String ENV_VAR_CLIENTS_CA_VALIDITY = "STRIMZI_CA_VALIDITY";
    public static final String ENV_VAR_CLIENTS_CA_RENEWAL = "STRIMZI_CA_RENEWAL";
    public static final String ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME = "STRIMZI_CLUSTER_CA_CERT_SECRET_NAME";
    public static final String ENV_VAR_EO_KEY_SECRET_NAME = "STRIMZI_EO_KEY_SECRET_NAME";
    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder().withTimeoutSeconds(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT)
            .withInitialDelaySeconds(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY).build();

    private String kafkaBootstrapServers;
    private String zookeeperConnect;
    private String watchedNamespace;
    private String resourceLabels;
    private long reconciliationIntervalMs;
    private long zookeeperSessionTimeoutMs;
    private int clientsCaValidityDays;
    private int clientsCaRenewalDays;
    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;

    /**
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected EntityUserOperator(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = userOperatorName(cluster);
        this.readinessPath = "/";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.livenessPath = "/";
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = EntityUserOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = EntityUserOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);

        this.ancillaryConfigMapName = metricAndLogConfigsName(cluster);
        this.logAndMetricsConfigVolumeName = "entity-user-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/user-operator/custom-config/";
        this.clientsCaValidityDays = CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS;
        this.clientsCaRenewalDays = CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setReconciliationIntervalMs(long reconciliationIntervalMs) {
        this.reconciliationIntervalMs = reconciliationIntervalMs;
    }

    public long getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    public void setClientsCaValidityDays(int clientsCaValidityDays) {
        this.clientsCaValidityDays = clientsCaValidityDays;
    }

    public long getClientsCaValidityDays() {
        return this.clientsCaValidityDays;
    }

    public void setClientsCaRenewalDays(int clientsCaRenewalDays) {
        this.clientsCaRenewalDays = clientsCaRenewalDays;
    }

    public long getClientsCaRenewalDays() {
        return this.clientsCaRenewalDays;
    }

    public void setZookeeperSessionTimeoutMs(long zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public long getZookeeperSessionTimeoutMs() {
        return zookeeperSessionTimeoutMs;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return String.format("%s:%d", "localhost", EntityUserOperatorSpec.DEFAULT_ZOOKEEPER_PORT);
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    public static String userOperatorName(String cluster) {
        return cluster + NAME_SUFFIX;
    }

    public static String metricAndLogConfigsName(String cluster) {
        return cluster + METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    /**
     * Get the name of the UO role binding given the name of the {@code cluster}.
     * @param cluster The cluster name.
     * @return The name of the role binding.
     */
    public static String roleBindingName(String cluster) {
        return "strimzi-" + cluster + "-entity-user-operator";
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "entityUserOperatorDefaultLoggingProperties";
    }

    @Override
    public String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }

    /**
     * Create an Entity User Operator from given desired resource
     *
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity User Operator one
     * @return Entity User Operator instance, null if not configured in the ConfigMap
     */
    public static EntityUserOperator fromCrd(Kafka kafkaAssembly) {
        EntityUserOperator result = null;
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();
        if (entityOperatorSpec != null) {

            EntityUserOperatorSpec userOperatorSpec = entityOperatorSpec.getUserOperator();
            if (userOperatorSpec != null) {

                String namespace = kafkaAssembly.getMetadata().getNamespace();
                result = new EntityUserOperator(kafkaAssembly);

                result.setOwnerReference(kafkaAssembly);
                String image = userOperatorSpec.getImage();
                if (image == null) {
                    image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_USER_OPERATOR_IMAGE, "strimzi/operator:latest");
                }
                result.setImage(image);
                result.setWatchedNamespace(userOperatorSpec.getWatchedNamespace() != null ? userOperatorSpec.getWatchedNamespace() : namespace);
                result.setReconciliationIntervalMs(userOperatorSpec.getReconciliationIntervalSeconds() * 1_000);
                result.setZookeeperSessionTimeoutMs(userOperatorSpec.getZookeeperSessionTimeoutSeconds() * 1_000);
                result.setLogging(userOperatorSpec.getLogging());
                result.setGcLoggingEnabled(userOperatorSpec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : userOperatorSpec.getJvmOptions().isGcLoggingEnabled());
                if (userOperatorSpec.getJvmOptions() != null) {
                    result.setJavaSystemProperties(userOperatorSpec.getJvmOptions().getJavaSystemProperties());
                }
                result.setJvmOptions(userOperatorSpec.getJvmOptions());
                result.setResources(userOperatorSpec.getResources());
                if (userOperatorSpec.getReadinessProbe() != null) {
                    result.setReadinessProbe(userOperatorSpec.getReadinessProbe());
                }
                if (userOperatorSpec.getLivenessProbe() != null) {
                    result.setLivenessProbe(userOperatorSpec.getLivenessProbe());
                }

                if (kafkaAssembly.getSpec().getClientsCa() != null) {
                    if (kafkaAssembly.getSpec().getClientsCa().getValidityDays() > 0) {
                        result.setClientsCaValidityDays(kafkaAssembly.getSpec().getClientsCa().getValidityDays());
                    }

                    if (kafkaAssembly.getSpec().getClientsCa().getRenewalDays() > 0) {
                        result.setClientsCaRenewalDays(kafkaAssembly.getSpec().getClientsCa().getRenewalDays());
                    }
                }
            }
        }
        return result;
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        return singletonList(new ContainerBuilder()
                .withName(USER_OPERATOR_CONTAINER_NAME)
                .withImage(getImage())
                .withArgs("/opt/strimzi/bin/user_operator_run.sh")
                .withEnv(getEnvVars())
                .withPorts(singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withLivenessProbe(createHttpProbe(livenessPath + "healthy", HEALTHCHECK_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(createHttpProbe(readinessPath + "ready", HEALTHCHECK_PORT_NAME, readinessProbeOptions))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build());
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(buildEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Long.toString(reconciliationIntervalMs)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Long.toString(zookeeperSessionTimeoutMs)));
        varList.add(buildEnvVar(ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME, KafkaCluster.clientsCaKeySecretName(cluster)));
        varList.add(buildEnvVar(ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME, KafkaCluster.clientsCaCertSecretName(cluster)));
        varList.add(buildEnvVar(ENV_VAR_CLIENTS_CA_NAMESPACE, namespace));
        varList.add(buildEnvVar(ENV_VAR_CLIENTS_CA_VALIDITY, Integer.toString(clientsCaValidityDays)));
        varList.add(buildEnvVar(ENV_VAR_CLIENTS_CA_RENEWAL, Integer.toString(clientsCaRenewalDays)));
        varList.add(buildEnvVar(ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME, KafkaCluster.clusterCaCertSecretName(cluster)));
        varList.add(buildEnvVar(ENV_VAR_EO_KEY_SECRET_NAME, EntityOperator.secretName(cluster)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        EntityOperator.javaOptions(varList, getJvmOptions(), javaSystemProperties);

        // Add shared environment variables used for all containers
        varList.addAll(getSharedEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    public List<Volume> getVolumes() {
        return singletonList(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
    }

    private List<VolumeMount> getVolumeMounts() {
        return asList(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
    }

    public RoleBinding generateRoleBinding(String namespace, String watchedNamespace) {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(EntityOperator.entityOperatorServiceAccountName(cluster))
                .withNamespace(namespace)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName(EntityOperator.EO_CLUSTER_ROLE_NAME)
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        RoleBinding rb = new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(roleBindingName(cluster))
                    .withNamespace(watchedNamespace)
                    .withOwnerReferences(createOwnerReference())
                    .withLabels(labels.toMap())
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(singletonList(ks))
                .build();

        return rb;
    }

    public void setContainerEnvVars(List<ContainerEnvVar> envVars) {
        templateContainerEnvVars = envVars;
    }

    public void setContainerSecurityContext(SecurityContext securityContext) {
        templateContainerSecurityContext = securityContext;
    }

    /**
     * Transforms properties to log4j2 properties file format and adds property for reloading the config
     * @param properties map with properties
     * @return modified string with monitorInterval
     */
    @Override
    public String createLog4jProperties(OrderedProperties properties) {
        if (!properties.asMap().keySet().contains("monitorInterval")) {
            properties.addPair("monitorInterval", "30");
        }
        return super.createLog4jProperties(properties);
    }
}
