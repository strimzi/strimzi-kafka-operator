/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicateResult;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

public class CreateUpdateConnectorCommand extends AbstractVerticle {
    private static final Logger log = LogManager.getLogger(KafkaConnectorAssemblyOperator.class.getName());

    Function<HttpResponse<Void>, ResponsePredicateResult> methodsPredicate = resp -> {
        int statusCode = resp.statusCode();
        if (statusCode == 200 || statusCode == 201) {
            return ResponsePredicateResult.success();
        }
        return ResponsePredicateResult.failure("Does not work");
    };

    @Override
    public void start() {
        WebClient.create(vertx)
                .put(config().getInteger("port"), config().getString("ip"), config().getString("path") + "/"
                        + config().getJsonObject("connector").getString("name") + "/config")
                .as(BodyCodec.jsonObject())
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                .expect(methodsPredicate)
                .sendJson(config().getJsonObject("connector").getJsonObject("config"), asyncResult -> {
                    if (asyncResult.succeeded()) {
                        log.info(asyncResult.result().body());
                    } else if (asyncResult.failed()) {
                        log.error(asyncResult.cause().getMessage());
                    }
                });
    }


}
