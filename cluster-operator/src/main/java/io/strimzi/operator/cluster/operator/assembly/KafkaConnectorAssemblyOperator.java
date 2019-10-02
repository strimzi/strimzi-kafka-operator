/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Assembly operator for a "Kafka Connector" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connector Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectorAssemblyOperator extends
        AbstractAssemblyOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector, Resource<KafkaConnector, DoneableKafkaConnector>> {
    private static final Logger log = LogManager.getLogger(KafkaConnectorAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";

    private final DeploymentOperator deploymentOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx       The Vertx instance
     * @param pfa         Platform features availability properties
     * @param certManager Certificate manager
     * @param supplier    Supplies the operators for different resources
     * @param config      ClusterOperator configuration. Used to get the
     *                    user-configured image pull policy and the secrets.
     */
    public KafkaConnectorAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                          CertManager certManager,
                                          ResourceOperatorSupplier supplier,
                                          ClusterOperatorConfig config) {
        super(vertx, pfa, ResourceType.KAFKACONNECTOR, certManager, supplier.kafkaConnectorOperator, supplier, config);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnector assemblyResource) {
        Future<Void> createOrUpdateFuture = Future.future();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        log.debug("{}: Creating/Updating Kafka Connector", reconciliation, name, namespace);

        JsonObject connectorConfigJson = new JsonObject().put("connector.class", assemblyResource.getSpec().getClassName())
                .put("tasks.max", assemblyResource.getSpec().getTasksMax())
                .put("topics", assemblyResource.getSpec().getTopics());
        assemblyResource.getSpec().getConfig().forEach(cf -> connectorConfigJson.put(cf.getName(), cf.getValue()));

        JsonObject connectorJson = new JsonObject().put("name", name).put("config", connectorConfigJson);
        JsonObject config = new JsonObject().put("ip", "my-connect-cluster-connect-api").put("port", 8083).put("path", "/connectors").put("connector", connectorJson);

        vertx.deployVerticle(new CreateUpdateConnectorCommand(), new DeploymentOptions().setConfig(config), ar -> {
            if (ar.succeeded()) {
                String id = ar.result();
                log.info("Successfully create update connector verticle deployed {}", id);
                createOrUpdateFuture.complete();
            } else {
                log.error("Error while deploying", ar.cause());
                createOrUpdateFuture.fail(ar.cause());
            }
        });

        return createOrUpdateFuture;
    }

    /**
     * Updates the Status field of the Kafka Bridge CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     * <p>
     * //     * @param kafkaBridgeAssembly The CR of Kafka Bridge
     * //     * @param reconciliation Reconciliation information
     * //     * @param desiredStatus The KafkaBridgeStatus which should be set
     * <p>
     * //     * @return
     */
//    Future<Void> updateStatus(KafkaBridge kafkaBridgeAssembly, Reconciliation reconciliation, KafkaBridgeStatus desiredStatus) {
//        Future<Void> updateStatusFuture = Future.future();
//
//        resourceOperator.getAsync(kafkaBridgeAssembly.getMetadata().getNamespace(), kafkaBridgeAssembly.getMetadata().getName()).setHandler(getRes -> {
//            if (getRes.succeeded()) {
//                KafkaBridge kafkaBridge = getRes.result();
//
//                if (kafkaBridge != null) {
//                    KafkaBridgeStatus currentStatus = kafkaBridge.getStatus();
//
//                    StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);
//
//                    if (!ksDiff.isEmpty()) {
//                        KafkaBridge resourceWithNewStatus = new KafkaBridgeBuilder(kafkaBridge).withStatus(desiredStatus).build();
//                        ((CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge>) resourceOperator).updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
//                            if (updateRes.succeeded()) {
//                                log.debug("{}: Completed status update", reconciliation);
//                                updateStatusFuture.complete();
//                            } else {
//                                log.error("{}: Failed to update status", reconciliation, updateRes.cause());
//                                updateStatusFuture.fail(updateRes.cause());
//                            }
//                        });
//                    } else {
//                        log.debug("{}: Status did not change", reconciliation);
//                        updateStatusFuture.complete();
//                    }
//                } else {
//                    log.error("{}: Current Kafka Bridge resource not found", reconciliation);
//                    updateStatusFuture.fail("Current Kafka Bridge resource not found");
//                }
//            } else {
//                log.error("{}: Failed to get the current Kafka Bridge resource and its status", reconciliation, getRes.cause());
//                updateStatusFuture.fail(getRes.cause());
//            }
//        });
//
//        return updateStatusFuture;
//    }

//    Future<ReconcileResult<ServiceAccount>> kafkaBridgeServiceAccount(String namespace, KafkaBridgeCluster bridge) {
//        return serviceAccountOperations.reconcile(namespace,
//                KafkaBridgeResources.serviceAccountName(bridge.getCluster()),
//                bridge.generateServiceAccount());
//    }

    Future<ReconcileResult<ServiceAccount>> kafkaConnectorServiceAccount(String namespace, String serviceAccountName) {
        ServiceAccount desiredServiceAccount = new ServiceAccountBuilder()
                .withNewMetadata()
                .withName(serviceAccountName)
                .withNamespace(namespace)
//                     .withOwnerReferences(createOwnerReference())
                // .withLabels(labels.toMap())
                .endMetadata()
                .build();
        return serviceAccountOperations.reconcile(namespace, serviceAccountName, desiredServiceAccount);
    }
}
