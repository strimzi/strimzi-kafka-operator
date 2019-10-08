/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.KafkaConnector;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicateResult;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

public class CreateUpdateConnectorCommand {
    private static final Logger log = LogManager.getLogger(CreateUpdateConnectorCommand.class.getName());

    private Function<HttpResponse<Void>, ResponsePredicateResult> methodsPredicate = resp -> {
        int statusCode = resp.statusCode();
        log.info(">>> statusCode: " + statusCode);
        if (statusCode == 200 || statusCode == 201) {
            return ResponsePredicateResult.success();
        }
        return ResponsePredicateResult.failure("Does not work");
    };

    public Future<Void> run(KafkaConnector kafkaConnector, String name, Vertx vertx) {
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
}
