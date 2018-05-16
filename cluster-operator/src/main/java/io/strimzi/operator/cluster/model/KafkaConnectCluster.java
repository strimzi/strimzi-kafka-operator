/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.extensions.RollingUpdateDeploymentBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaConnectCluster extends AbstractModel {

    // Port configuration
    protected static final int REST_API_PORT = 8083;
    protected static final String REST_API_PORT_NAME = "rest-api";

    private static final String NAME_SUFFIX = "-connect";

    // Configuration defaults
    protected static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_CONNECT_IMAGE", "strimzi/kafka-connect:latest");
    protected static final int DEFAULT_REPLICAS = 3;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 60;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Configuration keys (in ConfigMap)
    public static final String KEY_IMAGE = "image";
    public static final String KEY_REPLICAS = "nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "healthcheck-timeout";
    public static final String KEY_JVM_OPTIONS = "jvmOptions";
    public static final String KEY_RESOURCES = "resources";
    public static final String KEY_CONNECT_CONFIG = "connect-config";

    // Kafka Connect configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_CONNECT_CONFIGURATION = "KAFKA_CONNECT_CONFIGURATION";

    public static String kafkaConnectClusterName(String cluster) {
        return cluster + KafkaConnectCluster.NAME_SUFFIX;
    }

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected KafkaConnectCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaConnectClusterName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
    }

    /**
     * Create a Kafka Connect cluster from the related ConfigMap resource
     *
     * @param cm ConfigMap with cluster configuration
     * @return Kafka Connect cluster instance
     */
    public static KafkaConnectCluster fromConfigMap(ConfigMap cm) {
        KafkaConnectCluster kafkaConnect = new KafkaConnectCluster(cm.getMetadata().getNamespace(),
                cm.getMetadata().getName(),
                Labels.fromResource(cm));

        Map<String, String> data = cm.getData();
        kafkaConnect.setReplicas(Integer.parseInt(data.getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafkaConnect.setImage(data.getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafkaConnect.setResources(Resources.fromJson(data.get(KEY_RESOURCES)));
        kafkaConnect.setJvmOptions(JvmOptions.fromJson(data.get(KEY_JVM_OPTIONS)));
        kafkaConnect.setHealthCheckInitialDelay(Integer.parseInt(data.getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafkaConnect.setHealthCheckTimeout(Integer.parseInt(data.getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafkaConnect.setConfiguration(Utils.getKafkaConnectConfiguration(data, KEY_CONNECT_CONFIG));

        return kafkaConnect;
    }

    /**
     * Create a Kafka Connect cluster from the deployed Deployment resource
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @param dep The deployment from which to recover the cluster state
     * @return  Kafka Connect cluster instance
     */
    public static KafkaConnectCluster fromAssembly(
            String namespace, String cluster,
            Deployment dep) {
        if (dep == null) {
            return null;
        }

        KafkaConnectCluster kafkaConnect =  new KafkaConnectCluster(namespace, cluster, Labels.fromResource(dep));

        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        kafkaConnect.setImage(container.getImage());
        kafkaConnect.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        kafkaConnect.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        String connectConfiguration = containerEnvVars(container).getOrDefault(ENV_VAR_KAFKA_CONNECT_CONFIGURATION, "");
        kafkaConnect.setConfiguration(new KafkaConnectConfiguration(connectConfiguration));

        return kafkaConnect;
    }

    public Service generateService() {

        return createService("ClusterIP",
                Collections.singletonList(createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP")));
    }

    public Deployment generateDeployment() {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
                .build();

        return createDeployment(
                Collections.singletonList(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP")),
                createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout),
                createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout),
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                resources()
                );
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_CONFIGURATION, configuration.getConfiguration()));
        kafkaHeapOptions(varList, 1.0, 0L);

        return varList;
    }
}
