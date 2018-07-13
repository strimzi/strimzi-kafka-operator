/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.DoneableKafkaAssembly;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.vertx.core.Vertx;

public class ResourceOperatorSupplier {
    public final SecretOperator secretOperations;
    public final ServiceOperator serviceOperations;
    public final ZookeeperSetOperator zkSetOperations;
    public final KafkaSetOperator kafkaSetOperations;
    public final ConfigMapOperator configMapOperations;
    public final PvcOperator pvcOperations;
    public final DeploymentOperator deploymentOperations;
    public final ServiceAccountOperator serviceAccountOperator;
    public final ClusterRoleOperator cro;
    public final ClusterRoleBindingOperator crbo;
    public final CrdOperator<KubernetesClient, KafkaAssembly, KafkaAssemblyList, DoneableKafkaAssembly> kafkaOperator;

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        this(new ServiceOperator(vertx, client),
            new ZookeeperSetOperator(vertx, client, operationTimeoutMs),
            new KafkaSetOperator(vertx, client, operationTimeoutMs),
            new ConfigMapOperator(vertx, client),
            new SecretOperator(vertx, client),
            new PvcOperator(vertx, client),
            new DeploymentOperator(vertx, client),
            new ServiceAccountOperator(vertx, client),
            new ClusterRoleOperator(vertx, client),
            new ClusterRoleBindingOperator(vertx, client),
            new CrdOperator<>(vertx, client, KafkaAssembly .class, KafkaAssemblyList .class, DoneableKafkaAssembly .class));
    }

    public ResourceOperatorSupplier(ServiceOperator serviceOperations,
                                    ZookeeperSetOperator zkSetOperations,
                                    KafkaSetOperator kafkaSetOperations,
                                    ConfigMapOperator configMapOperations,
                                    SecretOperator secretOperations,
                                    PvcOperator pvcOperations,
                                    DeploymentOperator deploymentOperations,
                                    ServiceAccountOperator serviceAccountOperator,
                                    ClusterRoleOperator cro,
                                    ClusterRoleBindingOperator crbo,
                                    CrdOperator<KubernetesClient, KafkaAssembly, KafkaAssemblyList, DoneableKafkaAssembly> kafkaOperator) {
        this.serviceOperations = serviceOperations;
        this.zkSetOperations = zkSetOperations;
        this.kafkaSetOperations = kafkaSetOperations;
        this.configMapOperations = configMapOperations;
        this.secretOperations = secretOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.serviceAccountOperator = serviceAccountOperator;
        this.cro = cro;
        this.crbo = crbo;
        this.kafkaOperator = kafkaOperator;
    }
}
