/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ResourceType;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.client.predicate.ResponsePredicateResult;
import io.vertx.ext.web.codec.BodyCodec;

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
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnector assemblyResource) {
        Future<Void> createOrUpdateFuture = Future.future();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        log.info("{}: >>>> Creating/Updating Kafka Connector", reconciliation, name, namespace);
        update(assemblyResource, name, vertx)
                .compose(l -> list(assemblyResource, name, vertx))
                .setHandler(getRes -> {
                    if (getRes.succeeded()) {
                        log.info("Kafka Connector Create/Update Successfully");
                        createOrUpdateFuture.complete();
                    } else {
                        log.error("Kafka Connector Create/Update Failed!", getRes.cause());
                        createOrUpdateFuture.fail(getRes.cause());
                    }
                });

        return createOrUpdateFuture;
    }


    public Future<Void> list(KafkaConnector kafkaConnector, String name, Vertx vertx) {
        Future<Void> listPromise = Future.future();
        WebClient.create(vertx)
                .getAbs(kafkaConnector.getSpec().getConnectCluster().getUrl() + "/connectors")
                .as(BodyCodec.jsonArray())
                .putHeader("Accept", "application/json")
                .expect(ResponsePredicate.SC_OK)
                .send(asyncResult -> {
                    if (asyncResult.succeeded()) {
                        log.info("GET - Kafka Connector Success");
                        log.info(asyncResult.result().body());
                        listPromise.complete();
                    } else if (asyncResult.failed()) {
                        log.error(">>>>> GET - Kafka Connector Error", asyncResult.cause());
                        listPromise.fail(asyncResult.cause());
                    }
                });
        return listPromise;
    }

    public Future<Void> update(KafkaConnector kafkaConnector, String name, Vertx vertx) {
        Future<Void> updateRun = Future.future();
        log.info("Calling Kafka Connect API");
        JsonObject connectorConfigJson = new JsonObject().put("connector.class", kafkaConnector.getSpec().getClassName())
                .put("tasks.max", kafkaConnector.getSpec().getTasksMax())
                .put("topic", "test-topic");
        kafkaConnector.getSpec().getConfig().forEach(cf -> connectorConfigJson.put(cf.getName(), cf.getValue()));

        log.info(">>>> Connector config JSON: " + connectorConfigJson.encode());
        
        WebClient.create(vertx)
                .putAbs(kafkaConnector.getSpec().getConnectCluster().getUrl() + "/connectors/" + name + "/config")
                .as(BodyCodec.jsonObject())
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                .expect(methodsPredicate)
                .sendJson(connectorConfigJson, asyncResult -> {
                    if (asyncResult.succeeded()) {
                        log.info("PUT - Kafka Connector Success");
                        log.info(asyncResult.result().body());
                        updateRun.complete();
                    } else if (asyncResult.failed()) {
                        log.error(">>>>> PUT - Kafka Connector Error", asyncResult.cause());
                        updateRun.fail(asyncResult.cause());
                    }
                });

        return updateRun;
    }

    private Function<HttpResponse<Void>, ResponsePredicateResult> methodsPredicate = resp -> {
        int statusCode = resp.statusCode();
        log.info(">>> statusCode: " + statusCode);
        if (statusCode == 200 || statusCode == 201) {
            return ResponsePredicateResult.success();
        }
        return ResponsePredicateResult.failure("Does not work");
    };
}
