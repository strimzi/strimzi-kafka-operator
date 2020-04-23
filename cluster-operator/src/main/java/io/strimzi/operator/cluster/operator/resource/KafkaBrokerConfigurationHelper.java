/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * This class contains methods for getting current configuration from the kafka brokers asynchronously.
 */
public class KafkaBrokerConfigurationHelper {

    private static final Logger log = LogManager.getLogger(KafkaBrokerConfigurationHelper.class);

    public static Future<Admin> adminClient(SecretOperator secretOperations, AdminClientProvider adminClientProvider, String namespace, String cluster, int podId) {
        Promise<Admin> acPromise = Promise.promise();
        Future<Secret> clusterCaCertSecretFuture = secretOperations.getAsync(
                namespace, KafkaResources.clusterCaCertificateSecretName(cluster));
        Future<Secret> coKeySecretFuture = secretOperations.getAsync(
                namespace, ClusterOperator.secretName(cluster));
        String hostname = KafkaCluster.podDnsName(namespace, cluster, KafkaCluster.kafkaPodName(cluster, podId)) + ":" + KafkaCluster.REPLICATION_PORT;

        return CompositeFuture.join(clusterCaCertSecretFuture, coKeySecretFuture).compose(compositeFuture -> {
            Secret clusterCaCertSecret = compositeFuture.resultAt(0);
            if (clusterCaCertSecret == null) {
                return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaCertSecretName(cluster)));
            }
            Secret coKeySecret = compositeFuture.resultAt(1);
            if (coKeySecret == null) {
                return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(cluster)));
            }

            Admin ac;
            try {
                ac = adminClientProvider.createAdminClient(hostname, clusterCaCertSecret, coKeySecret, "cluster-operator");
                acPromise.complete(ac);
            } catch (RuntimeException e) {
                log.warn("Failed to create Admin Client. {}", e);
                acPromise.complete(null);
            }
            return acPromise.future();
        });
    }

    public static Future<Map<ConfigResource, Config>> getCurrentConfig(Vertx vertx, long operationTimeoutMs, int podId, Admin ac) {
        Promise<Map<ConfigResource, Config>> futRes = Promise.promise();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(podId));
        DescribeConfigsResult configs = ac.describeConfigs(Collections.singletonList(resource));
        Map<ConfigResource, Config> config = new HashMap<>();

        KafkaFuture<Map<ConfigResource, Config>> kafkaFuture = configs.all();
        Util.waitFor(vertx, "KafkaFuture to complete", "fetched", 1_000, operationTimeoutMs, () -> kafkaFuture.isDone());

        try {
            config.putAll(kafkaFuture.get());
            futRes.complete(config);
        } catch (InterruptedException | ExecutionException e) {
            futRes.fail(e);
        }

        return futRes.future();
    }
}
