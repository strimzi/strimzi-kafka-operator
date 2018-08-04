/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.DoneableKafkaUser;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.operator.user.operator.SimpleAclOperator;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class.getName());

    static {
        try {
            Crds.registerCustomKinds();
        } catch (Error | RuntimeException t) {
            log.error("Failed to register CRDs", t);
            throw t;
        }
    }

    public static void main(String[] args) {
        log.info("UserOperator is starting");
        UserOperatorConfig config = UserOperatorConfig.fromMap(System.getenv());
        Vertx vertx = Vertx.vertx();
        KubernetesClient client = new DefaultKubernetesClient();

        run(vertx, client, config).setHandler(ar -> {
            if (ar.failed()) {
                log.error("Unable to start operator", ar.cause());
                System.exit(1);
            }
        });
    }

    static Future<String> run(Vertx vertx, KubernetesClient client, UserOperatorConfig config) {
        printEnvInfo();
        OpenSslCertManager certManager = new OpenSslCertManager();
        SecretOperator secretOperations = new SecretOperator(vertx, client);
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> crdOperations = new CrdOperator<>(vertx, client, KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
        SimpleAclOperator aclOperations = new SimpleAclOperator(vertx, config.getZookeperConnect(), config.getZookeeperSessionTimeoutMs(), config.getZookeeperSessionTimeoutMs());

        KafkaUserOperator kafkaUserOperations = new KafkaUserOperator(vertx,
                certManager, crdOperations, secretOperations, aclOperations, config.getCaName(), config.getCaNamespace());

        Future<String> fut = Future.future();
        UserOperator operator = new UserOperator(config.getNamespace(),
                config,
                client,
                kafkaUserOperations);
        vertx.deployVerticle(operator,
            res -> {
                if (res.succeeded()) {
                    log.info("User Operator verticle started in namespace {}", config.getNamespace());
                } else {
                    log.error("User Operator verticle in namespace {} failed to start", config.getNamespace(), res.cause());
                    System.exit(1);
                }
                fut.completer().handle(res);
            });

        return fut;
    }

    static void printEnvInfo() {
        Map<String, String> m = new HashMap<>(System.getenv());
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry: m.entrySet()) {
            sb.append("\t").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        log.info("Using config:\n" + sb.toString());
    }
}
