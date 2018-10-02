/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Represents the Entity Operator deployment
 */
public class EntityOperator extends AbstractModel {

    private static final String NAME_SUFFIX = "-entity-operator";
    private static final String CERTS_SUFFIX = NAME_SUFFIX + "-certs";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_EO_CERTS_VOLUME_NAME = "eo-certs";
    protected static final String TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/eo-certs/";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";

    // Entity Operator configuration keys
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String ENV_VAR_TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";
    public static final String EO_CLUSTER_ROLE_NAME = "strimzi-entity-operator";

    private String zookeeperConnect;
    private EntityTopicOperator topicOperator;
    private EntityUserOperator userOperator;
    private TlsSidecar tlsSidecar;

    private boolean isDeployed;

    /**
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster overall cluster name
     * @param labels
     */
    protected EntityOperator(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = entityOperatorName(cluster);
        this.replicas = EntityOperatorSpec.DEFAULT_REPLICAS;
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    public void setTopicOperator(EntityTopicOperator topicOperator) {
        this.topicOperator = topicOperator;
    }

    public EntityTopicOperator getTopicOperator() {
        return topicOperator;
    }

    public void setUserOperator(EntityUserOperator userOperator) {
        this.userOperator = userOperator;
    }

    public EntityUserOperator getUserOperator() {
        return userOperator;
    }

    public static String entityOperatorName(String cluster) {
        return cluster + NAME_SUFFIX;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.serviceName(cluster) + ":" + EntityOperatorSpec.DEFAULT_ZOOKEEPER_PORT;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public static String secretName(String cluster) {
        return cluster + CERTS_SUFFIX;
    }

    public void setDeployed(boolean isDeployed) {
        this.isDeployed = isDeployed;
    }

    public boolean isDeployed() {
        return isDeployed;
    }

    /**
     * Create a Entity Operator from given desired resource
     *
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Operator one
     * @return Entity Operator instance, null if not configured in the ConfigMap
     */
    public static EntityOperator fromCrd(Kafka kafkaAssembly) {
        EntityOperator result = null;
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();
        if (entityOperatorSpec != null) {

            String namespace = kafkaAssembly.getMetadata().getNamespace();
            result = new EntityOperator(
                    namespace,
                    kafkaAssembly.getMetadata().getName(),
                    Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));

            result.setOwnerReference(kafkaAssembly);
            result.setUserAffinity(entityOperatorSpec.getAffinity());
            result.setTolerations(entityOperatorSpec.getTolerations());
            result.setTlsSidecar(entityOperatorSpec.getTlsSidecar());
            result.setTopicOperator(EntityTopicOperator.fromCrd(kafkaAssembly));
            result.setUserOperator(EntityUserOperator.fromCrd(kafkaAssembly));
            result.setDeployed(result.getTopicOperator() != null || result.getUserOperator() != null);
        }
        return result;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return null;
    }

    public Deployment generateDeployment() {

        if (!isDeployed()) {
            log.warn("Topic and/or User Operators not declared: Entity Operator will not be deployed");
            return null;
        }

        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                getVolumes()
        );
    }

    @Override
    protected List<Container> getContainers() {
        List<Container> containers = new ArrayList<>();

        if (topicOperator != null) {
            containers.addAll(topicOperator.getContainers());
        }
        if (userOperator != null) {
            containers.addAll(userOperator.getContainers());
        }

        String tlsSidecarImage = (tlsSidecar != null && tlsSidecar.getImage() != null) ?
                tlsSidecar.getImage() : EntityOperatorSpec.DEFAULT_TLS_SIDECAR_IMAGE;

        Resources tlsSidecarResources = (tlsSidecar != null) ? tlsSidecar.getResources() : null;

        TlsSidecarLogLevel tlsSidecarLogLevel = (tlsSidecar != null) ? tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE;

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withResources(resources(tlsSidecarResources))
                .withEnv(asList(buildEnvVar(ENV_VAR_TLS_SIDECAR_LOG_LEVEL, tlsSidecarLogLevel.toValue()),
                        buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect)))
                .withVolumeMounts(createVolumeMount(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                        createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT))
                .build();

        containers.add(tlsSidecarContainer);

        return containers;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (topicOperator != null) {
            volumeList.addAll(topicOperator.getVolumes());
        }
        if (userOperator != null) {
            volumeList.addAll(userOperator.getVolumes());
        }
        volumeList.add(createSecretVolume(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.secretName(cluster)));
        volumeList.add(createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.getClusterCaName(cluster)));
        return volumeList;
    }

    /**
     * Generate the Secret containing CA self-signed certificates for internal communication
     * It also contains the private key-certificate (signed by internal CA) for communicating with Zookeeper and Kafka
     * @return The generated Secret
     */
    public Secret generateSecret(ClusterCa clusterCa) {
        if (!isDeployed()) {
            return null;
        }
        Map<String, String> data = new HashMap<>();
        Secret secret = clusterCa.entityOperatorSecret();
        if (secret == null || clusterCa.certRenewed()) {
            log.debug("Generating certificates");
            try {
                Ca.log.debug("Entity Operator certificate to generate");
                CertAndKey eoCertAndKey = clusterCa.generateSignedCert(name, Ca.IO_STRIMZI);
                data.put("entity-operator.key", eoCertAndKey.keyAsBase64String());
                data.put("entity-operator.crt", eoCertAndKey.certAsBase64String());
            } catch (IOException e) {
                log.warn("Error while generating certificates", e);
            }

            log.debug("End generating certificates");
        } else {
            data.put("entity-operator.key", secret.getData().get("entity-operator.key"));
            data.put("entity-operator.crt", secret.getData().get("entity-operator.crt"));
        }
        return createSecret(EntityOperator.secretName(cluster), data);
    }

    /**
     * Get the name of the Entity Operator service account given the name of the {@code cluster}.
     */
    public static String entityOperatorServiceAccountName(String cluster) {
        return entityOperatorName(cluster);
    }

    @Override
    protected String getServiceAccountName() {
        return entityOperatorServiceAccountName(cluster);
    }

    public ServiceAccount generateServiceAccount() {

        if (!isDeployed()) {
            return null;
        }

        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(getServiceAccountName())
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .build();
    }
}
