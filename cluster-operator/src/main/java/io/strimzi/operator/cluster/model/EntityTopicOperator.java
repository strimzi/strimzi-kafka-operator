/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
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
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_TO_ADMIN_NAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_TO_ADMIN_NAME_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_TO_ADMIN_PASSWORD_KEY;
import static java.lang.String.format;

/**
 * Represents the Topic Operator deployment
 */
public class EntityTopicOperator extends AbstractModel implements SupportsLogging {
    protected static final String COMPONENT_TYPE = "entity-topic-operator";

    protected static final String TOPIC_OPERATOR_CONTAINER_NAME = "topic-operator";
    private static final String NAME_SUFFIX = "-entity-topic-operator";
    private static final String CERT_SECRET_KEY_NAME = "entity-operator";

    private static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "entity-topic-operator-metrics-and-logging";
    private static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/topic-operator/custom-config/";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";
    protected static final int CRUISE_CONTROL_API_PORT = 9090;

    // Topic Operator configuration keys
    /* test */ static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    /* test */ static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    /* test */ static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    /* test */ static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    /* test */ static final String ENV_VAR_SECURITY_PROTOCOL = "STRIMZI_SECURITY_PROTOCOL";

    /* test */ static final String ENV_VAR_TLS_ENABLED = "STRIMZI_TLS_ENABLED";

    // Volume name of the temporary volume used by the TO container
    // Because the container shares the pod with other containers, it needs to have unique name
    /* test */ static final String TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-to-tmp";
    
    /* test */ static final String ENV_VAR_CRUISE_CONTROL_ENABLED = "STRIMZI_CRUISE_CONTROL_ENABLED";
    /* test */ static final String ENV_VAR_CRUISE_CONTROL_RACK_ENABLED = "STRIMZI_CRUISE_CONTROL_RACK_AWARE";
    /* test */ static final String ENV_VAR_CRUISE_CONTROL_HOSTNAME = "STRIMZI_CRUISE_CONTROL_HOSTNAME";
    /* test */ static final String ENV_VAR_CRUISE_CONTROL_PORT = "STRIMZI_CRUISE_CONTROL_PORT";
    /* test */ static final String ENV_VAR_CRUISE_CONTROL_SSL_ENABLED = "STRIMZI_CRUISE_CONTROL_SSL_ENABLED";
    /* test */ static final String ENV_VAR_CRUISE_CONTROL_AUTH_ENABLED = "STRIMZI_CRUISE_CONTROL_AUTH_ENABLED";

    // Kafka bootstrap servers can't be specified in the JSON
    /* test */ String kafkaBootstrapServers;
    private boolean cruiseControlEnabled;
    private boolean rackAwarenessEnabled;

    private String watchedNamespace;
    /* test */ int reconciliationIntervalMs;
    /* test */ String resourceLabels;
    private ResourceTemplate templateRoleBinding;

    private LoggingModel logging;

    private Probe startupProbeOptions;

    /**
     * @param reconciliation   The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected EntityTopicOperator(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, resource.getMetadata().getName() + NAME_SUFFIX, COMPONENT_TYPE, sharedEnvironmentProvider);

        // create a default configuration
        this.kafkaBootstrapServers = KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT;
        this.reconciliationIntervalMs = EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);
    }

    /**
     * Create an Entity Topic Operator from given desired resource. When Topic Operator (Or Entity Operator) are not
     * enabled, it returns null.
     * @param reconciliation The reconciliation
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Topic Operator one
     *                      @param sharedEnvironmentProvider Shared environment provider
     *
     * @return Entity Topic Operator instance, null if not configured
     */
    public static EntityTopicOperator fromCrd(Reconciliation reconciliation,
                                              Kafka kafkaAssembly,
                                              SharedEnvironmentProvider sharedEnvironmentProvider) {
        if (kafkaAssembly.getSpec().getEntityOperator() != null
                && kafkaAssembly.getSpec().getEntityOperator().getTopicOperator() != null) {
            EntityTopicOperatorSpec topicOperatorSpec = kafkaAssembly.getSpec().getEntityOperator().getTopicOperator();
            EntityTopicOperator result = new EntityTopicOperator(reconciliation, kafkaAssembly, sharedEnvironmentProvider);

            String image = topicOperatorSpec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE, "quay.io/strimzi/operator:latest");
            }
            result.image = image;
            result.watchedNamespace = topicOperatorSpec.getWatchedNamespace() != null ? topicOperatorSpec.getWatchedNamespace() : kafkaAssembly.getMetadata().getNamespace();
            result.reconciliationIntervalMs = topicOperatorSpec.getReconciliationIntervalSeconds() * 1_000;
            result.logging = new LoggingModel(topicOperatorSpec, result.getClass().getSimpleName(), true, false);
            result.gcLoggingEnabled = topicOperatorSpec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : topicOperatorSpec.getJvmOptions().isGcLoggingEnabled();
            result.jvmOptions = topicOperatorSpec.getJvmOptions();
            result.resources = topicOperatorSpec.getResources();
            result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(topicOperatorSpec, EntityOperator.DEFAULT_HEALTHCHECK_OPTIONS);
            result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(topicOperatorSpec, EntityOperator.DEFAULT_HEALTHCHECK_OPTIONS);
            result.startupProbeOptions = ProbeUtils.extractStartupProbeOptionsOrDefault(topicOperatorSpec, new ProbeBuilder().withPeriodSeconds(10).withFailureThreshold(12).build());

            if (kafkaAssembly.getSpec().getEntityOperator().getTemplate() != null)  {
                result.templateRoleBinding = kafkaAssembly.getSpec().getEntityOperator().getTemplate().getTopicOperatorRoleBinding();
            }
            
            result.cruiseControlEnabled = kafkaAssembly.getSpec().getCruiseControl() != null;
            result.rackAwarenessEnabled = result.cruiseControlEnabled && kafkaAssembly.getSpec().getKafka().getRack() != null;

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
                ProbeUtils.httpProbe(livenessProbeOptions, "/healthy", HEALTHCHECK_PORT_NAME),
                ProbeUtils.httpProbe(readinessProbeOptions, "/ready", HEALTHCHECK_PORT_NAME),
                ProbeUtils.httpProbe(startupProbeOptions, "/healthy", HEALTHCHECK_PORT_NAME),
                imagePullPolicy,
                null
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_SECURITY_PROTOCOL, EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, Boolean.toString(gcLoggingEnabled)));
        
        // Add environment variables required for Cruise Control integration
        if (this.cruiseControlEnabled) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_ENABLED, Boolean.toString(cruiseControlEnabled)));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_RACK_ENABLED, Boolean.toString(rackAwarenessEnabled)));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_HOSTNAME, String.format("%s-cruise-control.%s.svc", cluster, namespace)));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_PORT, String.valueOf(CRUISE_CONTROL_API_PORT)));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_SSL_ENABLED, "true"));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_AUTH_ENABLED, "true"));
            // Truststore and API credentials are mounted in the container
        }
        
        JvmOptionUtils.javaOptions(varList, jvmOptions);

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    protected List<Volume> getVolumes() {
        return List.of(VolumeUtils.createConfigMapVolume(LOG_AND_METRICS_CONFIG_VOLUME_NAME, KafkaResources.entityTopicOperatorLoggingConfigMapName(cluster)));
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> result = new ArrayList<>();
        result.add(VolumeUtils.createTempDirVolumeMount(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        result.add(VolumeUtils.createVolumeMount(LOG_AND_METRICS_CONFIG_VOLUME_NAME, LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));
        result.add(VolumeUtils.createVolumeMount(EntityOperator.ETO_CERTS_VOLUME_NAME, EntityOperator.ETO_CERTS_VOLUME_MOUNT));
        result.add(VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
        if (this.cruiseControlEnabled) {
            result.add(VolumeUtils.createVolumeMount(EntityOperator.ETO_CC_API_VOLUME_NAME, EntityOperator.ETO_CC_API_VOLUME_MOUNT));
        }
        return Collections.unmodifiableList(result);
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
     * internal communication with Kafka.
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
        return CertUtils.buildTrustedCertificateSecret(reconciliation, clusterCa, secret, namespace, KafkaResources.entityTopicOperatorSecretName(cluster), componentName,
            CERT_SECRET_KEY_NAME, labels, ownerReference, isMaintenanceTimeWindowsSatisfied);
    }

    /**
     * Creates the Secret containing Cruise Control API auth credentials.
     * 
     * @param oldSecret The old secret.
     *                           
     * @return The generated Secret.
     */
    public Secret generateCruiseControlApiSecret(Secret oldSecret) {
        return ModelUtils.createSecret(KafkaResources.entityTopicOperatorCcApiSecretName(cluster), namespace, labels, ownerReference, 
            generateCruiseControlApiCredentials(oldSecret), Collections.emptyMap(), Collections.emptyMap());
    }
    
    private static Map<String, String> generateCruiseControlApiCredentials(Secret oldSecret) {
        if (oldSecret != null) {
            // The credentials should not change with every reconciliation
            // So if the secret with credentials already exists, we re-use the values
            // But we use the new secret to update labels etc. if needed
            var data = oldSecret.getData();
            var username = data.get(API_TO_ADMIN_NAME_KEY);
            var password = data.get(API_TO_ADMIN_PASSWORD_KEY);
            if (username == null || username.isBlank() || password == null || password.isBlank()) {
                throw new RuntimeException(format("Secret %s is invalid", oldSecret.getMetadata().getName()));
            } else {
                return data;
            }
        } else {
            PasswordGenerator passwordGenerator = new PasswordGenerator(16);
            String apiToAdminPassword = passwordGenerator.generate();
            return Map.of(
                API_TO_ADMIN_NAME_KEY, Util.encodeToBase64(API_TO_ADMIN_NAME),
                API_TO_ADMIN_PASSWORD_KEY, Util.encodeToBase64(apiToAdminPassword)
            );
        }
    }

    /**
     * @return Returns the namespace watched by the Topic Operator
     */
    public String watchedNamespace() {
        return watchedNamespace;
    }

    /**
     * Generates a metrics and logging ConfigMap according to the configuration. If this operand doesn't support logging
     * or metrics, they will nto be set.
     *
     * @param metricsAndLogging     The external CMs with logging and metrics configuration
     *
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsAndLogConfigMap(MetricsAndLogging metricsAndLogging) {
        return ConfigMapUtils
                .createConfigMap(
                        KafkaResources.entityTopicOperatorLoggingConfigMapName(cluster),
                        namespace,
                        labels,
                        ownerReference,
                        ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging)
                );
    }

    /**
     * @return  Logging Model instance for configuring logging
     */
    public LoggingModel logging()   {
        return logging;
    }
}
