/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.template.EntityOperatorTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.Main;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME;
import static io.strimzi.operator.cluster.model.EntityUserOperator.USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME;

/**
 * Represents the Entity Operator deployment
 */
@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
public class EntityOperator extends AbstractModel {
    protected static final String APPLICATION_NAME = "entity-operator";
    // Certificates for the Entity Topic Operator
    protected static final String ETO_CERTS_VOLUME_NAME = "eto-certs";
    protected static final String ETO_CERTS_VOLUME_MOUNT = "/etc/eto-certs/";
    // Certificates for the Entity User Operator
    protected static final String EUO_CERTS_VOLUME_NAME = "euo-certs";
    protected static final String EUO_CERTS_VOLUME_MOUNT = "/etc/euo-certs/";

    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";

    // Volume name of the temporary volume used by the TLS sidecar container
    // Because the container shares the pod with other containers, it needs to have unique name
    /*test*/ static final String TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-tls-sidecar-tmp";

    // Entity Operator configuration keys
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";

    protected static final String CO_ENV_VAR_CUSTOM_ENTITY_OPERATOR_POD_LABELS = "STRIMZI_CUSTOM_ENTITY_OPERATOR_LABELS";

    /* test */ String zookeeperConnect;
    private EntityTopicOperator topicOperator;
    private EntityUserOperator userOperator;
    private TlsSidecar tlsSidecar;
    private String tlsSidecarImage;

    private List<ContainerEnvVar> templateTlsSidecarContainerEnvVars;
    private SecurityContext templateTlsSidecarContainerSecurityContext;

    private boolean isDeployed;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_ENTITY_OPERATOR_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Kafka custom resource
     */
    protected EntityOperator(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
        this.name = KafkaResources.entityOperatorDeploymentName(cluster);
        this.replicas = EntityOperatorSpec.DEFAULT_REPLICAS;
        this.zookeeperConnect = ZookeeperCluster.serviceName(cluster) + ":" + ZookeeperCluster.CLIENT_TLS_PORT;
    }

    public EntityTopicOperator getTopicOperator() {
        return topicOperator;
    }

    public EntityUserOperator getUserOperator() {
        return userOperator;
    }

    public boolean isDeployed() {
        return isDeployed;
    }

    /**
     * Create an Entity Operator from given desired resource
     *
     * @param reconciliation The reconciliation
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Operator one
     * @param versions The versions.
     * @return Entity Operator instance, null if not configured in the ConfigMap
     */
    public static EntityOperator fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        EntityOperator result = null;
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();

        if (entityOperatorSpec != null) {
            result = new EntityOperator(reconciliation, kafkaAssembly);

            result.setOwnerReference(kafkaAssembly);

            EntityTopicOperator topicOperator = EntityTopicOperator.fromCrd(reconciliation, kafkaAssembly);
            EntityUserOperator userOperator = EntityUserOperator.fromCrd(reconciliation, kafkaAssembly);

            result.tlsSidecar = entityOperatorSpec.getTlsSidecar();
            result.topicOperator = topicOperator;
            result.userOperator = userOperator;
            result.isDeployed = result.getTopicOperator() != null || result.getUserOperator() != null;

            String tlsSideCarImage = entityOperatorSpec.getTlsSidecar() != null ? entityOperatorSpec.getTlsSidecar().getImage() : null;
            if (tlsSideCarImage == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                tlsSideCarImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            result.tlsSidecarImage = tlsSideCarImage;

            if (entityOperatorSpec.getTemplate() != null) {
                EntityOperatorTemplate template = entityOperatorSpec.getTemplate();

                if (template.getDeployment() != null && template.getDeployment().getMetadata() != null)  {
                    result.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                    result.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
                }

                ModelUtils.parsePodTemplate(result, template.getPod());

                if (template.getTopicOperatorContainer() != null && template.getTopicOperatorContainer().getEnv() != null) {
                    topicOperator.templateContainerEnvVars = template.getTopicOperatorContainer().getEnv();
                }

                if (template.getTopicOperatorContainer() != null && template.getTopicOperatorContainer().getSecurityContext() != null) {
                    topicOperator.templateContainerSecurityContext = template.getTopicOperatorContainer().getSecurityContext();
                }

                if (template.getUserOperatorContainer() != null && template.getUserOperatorContainer().getEnv() != null) {
                    userOperator.setContainerEnvVars(template.getUserOperatorContainer().getEnv());
                }

                if (template.getUserOperatorContainer() != null && template.getUserOperatorContainer().getSecurityContext() != null) {
                    userOperator.setContainerSecurityContext(template.getUserOperatorContainer().getSecurityContext());
                }

                if (template.getTlsSidecarContainer() != null && template.getTlsSidecarContainer().getEnv() != null) {
                    result.templateTlsSidecarContainerEnvVars = template.getTlsSidecarContainer().getEnv();
                }

                if (template.getTlsSidecarContainer() != null && template.getTlsSidecarContainer().getSecurityContext() != null) {
                    result.templateTlsSidecarContainerSecurityContext = template.getTlsSidecarContainer().getSecurityContext();
                }

                if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                    result.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                    result.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
                }
            }

            result.templatePodLabels = Util.mergeLabelsOrAnnotations(result.templatePodLabels, DEFAULT_POD_LABELS);
        }
        return result;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return null;
    }

    public Deployment generateDeployment(boolean isOpenShift, Map<String, String> annotations, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {

        if (!isDeployed()) {
            LOGGER.warnCr(reconciliation, "Topic and/or User Operators not declared: Entity Operator will not be deployed");
            return null;
        }

        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                annotations,
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets
        );
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(3);

        if (topicOperator != null) {
            containers.addAll(topicOperator.getContainers(imagePullPolicy));
        }
        if (userOperator != null) {
            containers.addAll(userOperator.getContainers(imagePullPolicy));
        }

        // The TLS Sidecar is only used by the Topic Operator. Therefore, when the Topic Operator is disabled, the TLS side should also be disabled.
        if (topicOperator != null) {
            String tlsSidecarImage = this.tlsSidecarImage;
            if (tlsSidecar != null && tlsSidecar.getImage() != null) {
                tlsSidecarImage = tlsSidecar.getImage();
            }

            Container tlsSidecarContainer = new ContainerBuilder()
                    .withName(TLS_SIDECAR_NAME)
                    .withImage(tlsSidecarImage)
                    .withCommand("/opt/stunnel/entity_operator_stunnel_run.sh")
                    .withLivenessProbe(ProbeGenerator.tlsSidecarLivenessProbe(tlsSidecar))
                    .withReadinessProbe(ProbeGenerator.tlsSidecarReadinessProbe(tlsSidecar))
                    .withResources(tlsSidecar != null ? tlsSidecar.getResources() : null)
                    .withEnv(getTlsSidecarEnvVars())
                    .withVolumeMounts(createTempDirVolumeMount(TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                            VolumeUtils.createVolumeMount(ETO_CERTS_VOLUME_NAME, ETO_CERTS_VOLUME_MOUNT),
                            VolumeUtils.createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT))
                    .withLifecycle(new LifecycleBuilder().withNewPreStop().withNewExec()
                            .withCommand("/opt/stunnel/entity_operator_stunnel_pre_stop.sh")
                            .endExec().endPreStop().build())
                    .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, tlsSidecarImage))
                    .withSecurityContext(templateTlsSidecarContainerSecurityContext)
                    .build();

            containers.add(tlsSidecarContainer);
        }
        return containers;
    }

    protected List<EnvVar> getTlsSidecarEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ModelUtils.tlsSidecarLogEnvVar(tlsSidecar));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateTlsSidecarContainerEnvVars);

        return varList;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(8);

        if (topicOperator != null) {
            volumeList.addAll(topicOperator.getVolumes());
            volumeList.add(createTempDirVolume(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            volumeList.add(VolumeUtils.createSecretVolume(ETO_CERTS_VOLUME_NAME, KafkaResources.entityTopicOperatorSecretName(cluster), isOpenShift));
        }

        if (userOperator != null) {
            volumeList.addAll(userOperator.getVolumes());
            volumeList.add(createTempDirVolume(USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            volumeList.add(VolumeUtils.createSecretVolume(EUO_CERTS_VOLUME_NAME, KafkaResources.entityUserOperatorSecretName(cluster), isOpenShift));
        }

        volumeList.add(createTempDirVolume(TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        volumeList.add(VolumeUtils.createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        return volumeList;
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaResources.entityOperatorDeploymentName(cluster);
    }

    @Override
    public ServiceAccount generateServiceAccount() {
        if (!isDeployed()) {
            return null;
        }
        return super.generateServiceAccount();
    }

    @Override
    protected String getRoleName() {
        return KafkaResources.entityOperatorDeploymentName(cluster);
    }

    /**
     * Read the entity operator ClusterRole, and use the rules to create a new Role.
     * This is done to avoid duplication of the rules set defined in source code.
     * If the namespace of the role is not the same as the namespace of the parent resource (Kafka CR), we do not set
     * the owner reference.
     *
     * @param ownerNamespace        The namespace of the parent resource (the Kafka CR)
     * @param namespace             The namespace this role will be located
     *
     * @return role for the entity operator
     */
    public Role generateRole(String ownerNamespace, String namespace) {
        List<PolicyRule> rules;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                    Main.class.getResourceAsStream("/cluster-roles/031-ClusterRole-strimzi-entity-operator.yaml"),
                    StandardCharsets.UTF_8)
            )
        ) {
            String yaml = br.lines().collect(Collectors.joining(System.lineSeparator()));
            ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
            ClusterRole cr = yamlReader.readValue(yaml, ClusterRole.class);
            rules = cr.getRules();
        } catch (IOException e) {
            LOGGER.errorCr(reconciliation, "Failed to read entity-operator ClusterRole.", e);
            throw new RuntimeException(e);
        }

        Role role = super.generateRole(namespace, rules);

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(ownerNamespace)) {
            role.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return role;
    }
}
