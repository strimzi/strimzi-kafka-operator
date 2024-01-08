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
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the User Operator deployment
 */
public class EntityUserOperator extends AbstractModel implements SupportsLogging {
    protected static final String COMPONENT_TYPE = "entity-user-operator";
    
    protected static final String USER_OPERATOR_CONTAINER_NAME = "user-operator";
    private static final String NAME_SUFFIX = "-entity-user-operator";
    private static final String CERT_SECRET_KEY_NAME = "entity-operator";

    private static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "entity-user-operator-metrics-and-logging";
    private static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/user-operator/custom-config/";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8081;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // User Operator configuration keys
    /* test */ static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_LABELS";
    /* test */ static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    /* test */ static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    /* test */ static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    /* test */ static final String ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME = "STRIMZI_CA_CERT_NAME";
    /* test */ static final String ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME = "STRIMZI_CA_KEY_NAME";
    /* test */ static final String ENV_VAR_CLIENTS_CA_NAMESPACE = "STRIMZI_CA_NAMESPACE";
    /* test */ static final String ENV_VAR_CLIENTS_CA_VALIDITY = "STRIMZI_CA_VALIDITY";
    /* test */ static final String ENV_VAR_CLIENTS_CA_RENEWAL = "STRIMZI_CA_RENEWAL";
    /* test */ static final String ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME = "STRIMZI_CLUSTER_CA_CERT_SECRET_NAME";
    /* test */ static final String ENV_VAR_EO_KEY_SECRET_NAME = "STRIMZI_EO_KEY_SECRET_NAME";
    /* test */ static final String ENV_VAR_SECRET_PREFIX = "STRIMZI_SECRET_PREFIX";
    /* test */ static final String ENV_VAR_ACLS_ADMIN_API_SUPPORTED = "STRIMZI_ACLS_ADMIN_API_SUPPORTED";
    /* test */ static final String ENV_VAR_MAINTENANCE_TIME_WINDOWS = "STRIMZI_MAINTENANCE_TIME_WINDOWS";

    // Volume name of the temporary volume used by the UO container
    // Because the container shares the pod with other containers, it needs to have unique name
    /*test*/ static final String USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-uo-tmp";

    /* test */ String kafkaBootstrapServers;
    private String watchedNamespace;
    /* test */ String resourceLabels;
    /* test */ String secretPrefix;
    /* test */ long reconciliationIntervalMs;
    /* test */ int clientsCaValidityDays;
    /* test */ int clientsCaRenewalDays;
    private ResourceTemplate templateRoleBinding;

    private boolean aclsAdminApiSupported = false;
    private List<String> maintenanceWindows;
    private LoggingModel logging;

    /**
     * @param reconciliation   The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected EntityUserOperator(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, resource.getMetadata().getName() + NAME_SUFFIX, COMPONENT_TYPE, sharedEnvironmentProvider);

        // create a default configuration
        this.kafkaBootstrapServers = KafkaResources.bootstrapServiceName(cluster) + ":" + EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT;
        this.reconciliationIntervalMs = EntityUserOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.secretPrefix = EntityUserOperatorSpec.DEFAULT_SECRET_PREFIX;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);

        this.clientsCaValidityDays = CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS;
        this.clientsCaRenewalDays = CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS;
    }

    /**
     * Create an Entity User Operator from given desired resource. When User Operator (Or Entity Operator) are not
     * enabled, it returns null.
     *
     * @param reconciliation                The reconciliation
     * @param kafkaAssembly                 Desired resource with cluster configuration containing the Entity User Operator one
     * @param sharedEnvironmentProvider     Shared environment provider
     *
     * @return Entity User Operator instance, null if not configured
     */
    public static EntityUserOperator fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, SharedEnvironmentProvider sharedEnvironmentProvider) {
        if (kafkaAssembly.getSpec().getEntityOperator() != null
                && kafkaAssembly.getSpec().getEntityOperator().getUserOperator() != null) {
            EntityUserOperatorSpec userOperatorSpec = kafkaAssembly.getSpec().getEntityOperator().getUserOperator();
            EntityUserOperator result = new EntityUserOperator(reconciliation, kafkaAssembly, sharedEnvironmentProvider);

            String image = userOperatorSpec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_USER_OPERATOR_IMAGE, "quay.io/strimzi/operator:latest");
            }
            result.image = image;

            result.watchedNamespace = userOperatorSpec.getWatchedNamespace() != null ? userOperatorSpec.getWatchedNamespace() : kafkaAssembly.getMetadata().getNamespace();
            result.reconciliationIntervalMs = userOperatorSpec.getReconciliationIntervalSeconds() * 1_000;
            result.secretPrefix = userOperatorSpec.getSecretPrefix() == null ? EntityUserOperatorSpec.DEFAULT_SECRET_PREFIX : userOperatorSpec.getSecretPrefix();
            result.logging = new LoggingModel(userOperatorSpec, result.getClass().getSimpleName(), true, false);
            result.gcLoggingEnabled = userOperatorSpec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : userOperatorSpec.getJvmOptions().isGcLoggingEnabled();
            result.jvmOptions = userOperatorSpec.getJvmOptions();
            result.resources = userOperatorSpec.getResources();
            result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(userOperatorSpec, EntityOperator.DEFAULT_HEALTHCHECK_OPTIONS);
            result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(userOperatorSpec, EntityOperator.DEFAULT_HEALTHCHECK_OPTIONS);

            if (kafkaAssembly.getSpec().getEntityOperator().getTemplate() != null)  {
                result.templateRoleBinding = kafkaAssembly.getSpec().getEntityOperator().getTemplate().getUserOperatorRoleBinding();
            }

            if (kafkaAssembly.getSpec().getClientsCa() != null) {
                if (kafkaAssembly.getSpec().getClientsCa().getValidityDays() > 0) {
                    result.clientsCaValidityDays = kafkaAssembly.getSpec().getClientsCa().getValidityDays();
                }

                if (kafkaAssembly.getSpec().getClientsCa().getRenewalDays() > 0) {
                    result.clientsCaRenewalDays = kafkaAssembly.getSpec().getClientsCa().getRenewalDays();
                }
            }

            if (kafkaAssembly.getSpec().getKafka().getAuthorization() != null) {
                // Indicates whether the Kafka Admin API for ACL management are supported by the configured authorizer
                // plugin. This information is passed to the User Operator.
                result.aclsAdminApiSupported = kafkaAssembly.getSpec().getKafka().getAuthorization().supportsAdminApi();
            }

            if (kafkaAssembly.getSpec().getMaintenanceTimeWindows() != null)    {
                result.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
            }

            return result;
        } else {
            return null;
        }
    }

    protected Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                USER_OPERATOR_CONTAINER_NAME,
                image,
                List.of("/opt/strimzi/bin/user_operator_run.sh"),
                securityProvider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                List.of(ContainerUtils.createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT)),
                getVolumeMounts(),
                ProbeUtils.httpProbe(livenessProbeOptions, "/healthy", HEALTHCHECK_PORT_NAME),
                ProbeUtils.httpProbe(readinessProbeOptions, "/ready", HEALTHCHECK_PORT_NAME),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Long.toString(reconciliationIntervalMs)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME, KafkaResources.clientsCaKeySecretName(cluster)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME, KafkaResources.clientsCaCertificateSecretName(cluster)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CLIENTS_CA_NAMESPACE, namespace));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CLIENTS_CA_VALIDITY, Integer.toString(clientsCaValidityDays)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CLIENTS_CA_RENEWAL, Integer.toString(clientsCaRenewalDays)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME, KafkaCluster.clusterCaCertSecretName(cluster)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_EO_KEY_SECRET_NAME, KafkaResources.entityUserOperatorSecretName(cluster)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_SECRET_PREFIX, secretPrefix));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ACLS_ADMIN_API_SUPPORTED, String.valueOf(aclsAdminApiSupported)));
        JvmOptionUtils.javaOptions(varList, jvmOptions);

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        // if maintenance time windows are set, we pass them as environment variable
        if (maintenanceWindows != null && !maintenanceWindows.isEmpty())    {
            // The Cron expressions can contain commas -> we use semi-colon as delimiter
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_MAINTENANCE_TIME_WINDOWS, String.join(";", maintenanceWindows)));
        }

        return varList;
    }

    protected List<Volume> getVolumes() {
        return List.of(VolumeUtils.createConfigMapVolume(LOG_AND_METRICS_CONFIG_VOLUME_NAME, KafkaResources.entityUserOperatorLoggingConfigMapName(cluster)));
    }

    private List<VolumeMount> getVolumeMounts() {
        return List.of(VolumeUtils.createTempDirVolumeMount(USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                VolumeUtils.createVolumeMount(LOG_AND_METRICS_CONFIG_VOLUME_NAME, LOG_AND_METRICS_CONFIG_VOLUME_MOUNT),
                VolumeUtils.createVolumeMount(EntityOperator.EUO_CERTS_VOLUME_NAME, EntityOperator.EUO_CERTS_VOLUME_MOUNT),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
    }

    /**
     * Generates the User Operator Role Binding
     *
     * @param namespace         Namespace where the User Operator is deployed
     * @param watchedNamespace  Namespace which the User Operator is watching
     *
     * @return  Role Binding for the User Operator
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
                .createRoleBinding(KafkaResources.entityUserOperatorRoleBinding(cluster), watchedNamespace, roleRef, List.of(subject), labels, ownerReference, templateRoleBinding);

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(watchedNamespace)) {
            rb.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return rb;
    }

    /**
     * Generate the Secret containing the Entity User Operator certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Entity User Operator private key.
     *
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret.
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Secret secret = clusterCa.entityUserOperatorSecret();
        return CertUtils.buildTrustedCertificateSecret(reconciliation, clusterCa, secret, namespace, KafkaResources.entityUserOperatorSecretName(cluster), componentName,
            CERT_SECRET_KEY_NAME, labels, ownerReference, isMaintenanceTimeWindowsSatisfied);
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
                        KafkaResources.entityUserOperatorLoggingConfigMapName(cluster),
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
