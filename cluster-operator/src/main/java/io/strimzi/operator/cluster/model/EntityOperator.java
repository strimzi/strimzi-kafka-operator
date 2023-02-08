/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.template.EntityOperatorTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.Main;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
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

import static io.strimzi.api.kafka.model.template.DeploymentStrategy.RECREATE;
import static io.strimzi.operator.cluster.model.EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME;
import static io.strimzi.operator.cluster.model.EntityUserOperator.USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME;

/**
 * Represents the Entity Operator deployment
 */
@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
public class EntityOperator extends AbstractModel {
    protected static final String COMPONENT_TYPE = "entity-operator";
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
    /* test */ static final String TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-tls-sidecar-tmp";

    // Entity Operator configuration keys
    /* test */ static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";

    protected static final String CO_ENV_VAR_CUSTOM_ENTITY_OPERATOR_POD_LABELS = "STRIMZI_CUSTOM_ENTITY_OPERATOR_LABELS";

    /* test */ String zookeeperConnect;
    private EntityTopicOperator topicOperator;
    private EntityUserOperator userOperator;
    private TlsSidecar tlsSidecar;
    private String tlsSidecarImage;

    private ResourceTemplate templateRole;
    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;

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
        super(reconciliation, resource, KafkaResources.entityOperatorDeploymentName(resource.getMetadata().getName()), COMPONENT_TYPE);

        this.replicas = EntityOperatorSpec.DEFAULT_REPLICAS;
        this.zookeeperConnect = KafkaResources.zookeeperServiceName(cluster) + ":" + ZookeeperCluster.CLIENT_TLS_PORT;
    }

    /**
     * Create an Entity Operator from given desired resource
     *
     * @param reconciliation The reconciliation
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Operator one
     * @param versions The versions.
     * @param kraftEnabled Indicates whether KRaft is used in the Kafka cluster
     *
     * @return Entity Operator instance, null if not configured in the ConfigMap
     */
    public static EntityOperator fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, boolean kraftEnabled) {
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();

        if (entityOperatorSpec != null
                && (entityOperatorSpec.getUserOperator() != null || entityOperatorSpec.getTopicOperator() != null)) {
            EntityOperator result = new EntityOperator(reconciliation, kafkaAssembly);

            EntityTopicOperator topicOperator = EntityTopicOperator.fromCrd(reconciliation, kafkaAssembly);
            EntityUserOperator userOperator = EntityUserOperator.fromCrd(reconciliation, kafkaAssembly, kraftEnabled);

            result.tlsSidecar = entityOperatorSpec.getTlsSidecar();
            result.topicOperator = topicOperator;
            result.userOperator = userOperator;

            String tlsSideCarImage = entityOperatorSpec.getTlsSidecar() != null ? entityOperatorSpec.getTlsSidecar().getImage() : null;
            if (tlsSideCarImage == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                tlsSideCarImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            result.tlsSidecarImage = tlsSideCarImage;

            if (entityOperatorSpec.getTemplate() != null) {
                EntityOperatorTemplate template = entityOperatorSpec.getTemplate();

                result.templateRole = template.getEntityOperatorRole();
                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateServiceAccount = template.getServiceAccount();
                result.templateContainer = template.getTlsSidecarContainer();

                if (topicOperator != null) {
                    topicOperator.templateContainer = template.getTopicOperatorContainer();
                }

                if (userOperator != null) {
                    userOperator.templateContainer = template.getUserOperatorContainer();
                }
            }

            return result;
        } else {
            return null;
        }
    }

    /**
     * @return  The Topic Operator model
     */
    public EntityTopicOperator topicOperator() {
        return topicOperator;
    }

    /**
     * @return  The User Operator model
     */
    public EntityUserOperator userOperator() {
        return userOperator;
    }

    /**
     * Generates the Entity Operator deployment
     *
     * @param isOpenShift       Flag which identifies if we are running on OpenShift
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  Image pull secrets
     *
     * @return  Kubernetes Deployment with the Entity Operator
     */
    public Deployment generateDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return WorkloadUtils.createDeployment(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateDeployment,
                replicas,
                WorkloadUtils.deploymentStrategy(RECREATE), // we intentionally ignore the template here as EO doesn't support RU strategy
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        Map.of(),
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        createContainers(imagePullPolicy),
                        getVolumes(isOpenShift),
                        imagePullSecrets,
                        securityProvider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    /* test */ List<Container> createContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(3);

        if (topicOperator != null) {
            containers.add(topicOperator.createContainer(imagePullPolicy));
        }
        if (userOperator != null) {
            containers.add(userOperator.createContainer(imagePullPolicy));
        }

        // The TLS Sidecar is only used by the Topic Operator. Therefore, when the Topic Operator is disabled, the TLS side should also be disabled.
        if (topicOperator != null) {
            String tlsSidecarImage = this.tlsSidecarImage;
            if (tlsSidecar != null && tlsSidecar.getImage() != null) {
                tlsSidecarImage = tlsSidecar.getImage();
            }

            Container tlsSidecarContainer = ContainerUtils.createContainer(
                    TLS_SIDECAR_NAME,
                    tlsSidecarImage,
                    List.of("/opt/stunnel/entity_operator_stunnel_run.sh"),
                    securityProvider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                    tlsSidecar != null ? tlsSidecar.getResources() : null,
                    getTlsSidecarEnvVars(),
                    null,
                    List.of(VolumeUtils.createTempDirVolumeMount(TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                            VolumeUtils.createVolumeMount(ETO_CERTS_VOLUME_NAME, ETO_CERTS_VOLUME_MOUNT),
                            VolumeUtils.createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT)),
                    ProbeGenerator.tlsSidecarLivenessProbe(tlsSidecar),
                    ProbeGenerator.tlsSidecarReadinessProbe(tlsSidecar),
                    null,
                    imagePullPolicy,
                    new LifecycleBuilder().withNewPreStop().withNewExec().withCommand("/opt/stunnel/entity_operator_stunnel_pre_stop.sh").endExec().endPreStop().build()
            );

            containers.add(tlsSidecarContainer);
        }

        return containers;
    }

    protected List<EnvVar> getTlsSidecarEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ModelUtils.tlsSidecarLogEnvVar(tlsSidecar));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));

        // Add shared environment variables used for all containers
        varList.addAll(ContainerUtils.requiredEnvVars());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(8);

        if (topicOperator != null) {
            volumeList.addAll(topicOperator.getVolumes());
            volumeList.add(VolumeUtils.createTempDirVolume(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, templatePod));
            volumeList.add(VolumeUtils.createSecretVolume(ETO_CERTS_VOLUME_NAME, KafkaResources.entityTopicOperatorSecretName(cluster), isOpenShift));
        }

        if (userOperator != null) {
            volumeList.addAll(userOperator.getVolumes());
            volumeList.add(VolumeUtils.createTempDirVolume(USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, templatePod));
            volumeList.add(VolumeUtils.createSecretVolume(EUO_CERTS_VOLUME_NAME, KafkaResources.entityUserOperatorSecretName(cluster), isOpenShift));
        }

        volumeList.add(VolumeUtils.createTempDirVolume(TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, templatePod));
        volumeList.add(VolumeUtils.createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        return volumeList;
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

        Role role = RbacUtils.createRole(componentName, namespace, rules, labels, ownerReference, templateRole);

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(ownerNamespace)) {
            role.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return role;
    }
}
