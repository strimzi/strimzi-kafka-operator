/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class contains methods for getting current configuration from the kafka brokers asynchronously.
 */
public class KafkaBrokerConfigurationHelper {

    private final AdminClientProvider adminClientProvider;
    protected final SecretOperator secretOperations;
    private static final Logger log = LogManager.getLogger(KafkaBrokerConfigurationHelper.class);

    public KafkaBrokerConfigurationHelper(AdminClientProvider adminClientProvider, SecretOperator secretOperations) {
        this.adminClientProvider = adminClientProvider;
        this.secretOperations = secretOperations;
    }

    public Future<Admin> adminClient(StatefulSet sts, int podId) {
        Promise<Admin> acPromise = Promise.promise();
        String cluster = sts.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String namespace = sts.getMetadata().getNamespace();
        Future<Secret> clusterCaKeySecretFuture = secretOperations.getAsync(
                namespace, KafkaResources.clusterCaCertificateSecretName(cluster));
        Future<Secret> coKeySecretFuture = secretOperations.getAsync(
                namespace, ClusterOperator.secretName(cluster));
        String hostname = KafkaCluster.podDnsName(namespace, cluster, KafkaCluster.kafkaPodName(cluster, podId)) + ":" + KafkaCluster.REPLICATION_PORT;

        return CompositeFuture.join(clusterCaKeySecretFuture, coKeySecretFuture).compose(compositeFuture -> {
            Secret clusterCaKeySecret = compositeFuture.resultAt(0);
            if (clusterCaKeySecret == null) {
                return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaKeySecretName(cluster)));
            }
            Secret coKeySecret = compositeFuture.resultAt(1);
            if (coKeySecret == null) {
                return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(cluster)));
            }

            Admin ac;
            try {
                log.info("about to create an admin client");
                ac = adminClientProvider.createAdminClient(hostname, clusterCaKeySecret, coKeySecret, "cluster-operator");
                acPromise.complete(ac);
            } catch (RuntimeException e) {
                log.warn("Failed to create Admin Client. {}", e);
                acPromise.complete(null);
            }
            return acPromise.future();
        });
    }

    public Future<Map<ConfigResource, Config>> getCurrentConfig(int podId, Admin ac) {
        Promise<Map<ConfigResource, Config>> futRes = Promise.promise();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(podId));
        DescribeConfigsResult configs = ac.describeConfigs(Collections.singletonList(resource));
        Map<ConfigResource, Config> config = new HashMap<>();
        try {
            config = configs.all().get(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Error while getting broker {} config {}", podId, e.getMessage());
        }
        Properties result = new Properties();
        config.forEach((key, value) -> {
            value.entries().forEach(entry -> {
                String val = entry.value() == null ? "null" : entry.value();
                result.put(entry.name(), val);
            });
        });
        futRes.complete(config);
        return futRes.future();
    }
}
