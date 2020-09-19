/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.micrometer.backends.BackendRegistries;

/**
 * An "operator" for managing assemblies of various types <em>in a particular namespace</em>.
 */
public class UserOperator extends AbstractVerticle {

    private static final Logger log = LogManager.getLogger(UserOperator.class.getName());

    private static final int HEALTH_SERVER_PORT = 8081;

    private final KubernetesClient client;
    private final String namespace;
    private final long reconciliationInterval;
    private final KafkaUserOperator kafkaUserOperator;

    private final PrometheusMeterRegistry metrics;

    private Watch watch;
    private long reconcileTimer;

    public UserOperator(String namespace,
                        UserOperatorConfig config,
                        KubernetesClient client,
                        KafkaUserOperator kafkaUserOperator) {
        log.info("Creating UserOperator for namespace {}", namespace);
        this.namespace = namespace;
        this.reconciliationInterval = config.getReconciliationIntervalMs();
        this.client = client;
        this.kafkaUserOperator = kafkaUserOperator;
        this.metrics = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
    }

    @Override
    public void start(Promise<Void> start) {
        log.info("Starting UserOperator for namespace {}", namespace);

        // Configure the executor here, but it is used only in other places
        getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 10, TimeUnit.SECONDS.toNanos(120));

        kafkaUserOperator.createWatch(namespace, kafkaUserOperator.recreateWatch(namespace))
            .compose(w -> {
                log.info("Started operator for {} kind", "KafkaUser");
                watch = w;

                log.info("Setting up periodic reconciliation for namespace {}", namespace);
                this.reconcileTimer = vertx.setPeriodic(this.reconciliationInterval, res2 -> {
                    log.info("Triggering periodic reconciliation for namespace {}...", namespace);
                    reconcileAll("timer");
                });

                return startHealthServer().map((Void) null);
            })
            .onComplete(start);
    }

    @Override
    public void stop(Promise<Void> stop) {
        log.info("Stopping UserOperator for namespace {}", namespace);
        vertx.cancelTimer(reconcileTimer);

        if (watch != null) {
            watch.close();
        }

        client.close();
        stop.complete();
    }

    /**
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcileAll(String trigger) {
        kafkaUserOperator.reconcileAll(trigger, namespace, ignored -> { });
    }

    /**
     * Start an HTTP health server
     */
    private Future<HttpServer> startHealthServer() {
        Promise<HttpServer> result = Promise.promise();
        this.vertx.createHttpServer()
                .requestHandler(request -> {
                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/metrics")) {
                        request.response().setStatusCode(200).end(metrics.scrape());
                    }
                })
                .listen(HEALTH_SERVER_PORT, ar -> {
                    if (ar.succeeded()) {
                        log.info("UserOperator is now ready (health server listening on {})", HEALTH_SERVER_PORT);
                    } else {
                        log.error("Unable to bind health server on {}", HEALTH_SERVER_PORT, ar.cause());
                    }
                    result.handle(ar);
                });
        return result.future();
    }

}
