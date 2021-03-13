/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.operator.user.operator.KafkaUserQuotasOperator;
import io.strimzi.operator.user.operator.ScramShaCredentials;
import io.strimzi.operator.user.operator.ScramShaCredentialsOperator;
import io.strimzi.operator.user.operator.SimpleAclOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;

@SuppressFBWarnings("DM_EXIT")
@SuppressWarnings("deprecation")
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
        log.info("UserOperator {} is starting", Main.class.getPackage().getImplementationVersion());
        UserOperatorConfig config = UserOperatorConfig.fromMap(System.getenv());
        //Setup Micrometer metrics options
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(options);

        KubernetesClient client = new DefaultKubernetesClient();
        AdminClientProvider adminClientProvider = new DefaultAdminClientProvider();

        run(vertx, client, adminClientProvider, config).onComplete(ar -> {
            if (ar.failed()) {
                log.error("Unable to start operator", ar.cause());
                System.exit(1);
            }
        });
    }

    static Future<String> run(Vertx vertx, KubernetesClient client, AdminClientProvider adminClientProvider, UserOperatorConfig config) {
        Util.printEnvInfo();
        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        OpenSslCertManager certManager = new OpenSslCertManager();
        SecretOperator secretOperations = new SecretOperator(vertx, client);
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> crdOperations = new CrdOperator<>(vertx, client, KafkaUser.class, KafkaUserList.class, Crds.kafkaUser());
        return createAdminClient(adminClientProvider, config, secretOperations)
                .compose(adminClient -> {
                    SimpleAclOperator aclOperations = new SimpleAclOperator(vertx, adminClient);
                    ScramShaCredentials scramShaCredentials = new ScramShaCredentials(config.getZookeperConnect(), (int) config.getZookeeperSessionTimeoutMs());
                    ScramShaCredentialsOperator scramShaCredentialsOperator = new ScramShaCredentialsOperator(vertx, scramShaCredentials);
                    KafkaUserQuotasOperator quotasOperator = new KafkaUserQuotasOperator(vertx, adminClient);

                    KafkaUserOperator kafkaUserOperations = new KafkaUserOperator(vertx,
                            certManager, crdOperations,
                            config.getLabels(),
                            secretOperations, scramShaCredentialsOperator, quotasOperator, aclOperations, config.getCaCertSecretName(), config.getCaKeySecretName(), config.getCaNamespace(),
                            config.getSecretPrefix());

                    Promise<String> promise = Promise.promise();
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
                            promise.handle(res);
                        });
                    return promise.future();
                });
    }

    private static Future<Admin> createAdminClient(AdminClientProvider adminClientProvider, UserOperatorConfig config, SecretOperator secretOperations) {
        Promise<Admin> promise = Promise.promise();

        Future<Secret> clusterCaCertSecretFuture;
        if (config.getClusterCaCertSecretName() != null && !config.getClusterCaCertSecretName().isEmpty()) {
            clusterCaCertSecretFuture = secretOperations.getAsync(config.getCaNamespace(), config.getClusterCaCertSecretName());
        } else {
            clusterCaCertSecretFuture = Future.succeededFuture(null);
        }
        Future<Secret> eoKeySecretFuture;
        if (config.getEoKeySecretName() != null && !config.getEoKeySecretName().isEmpty()) {
            eoKeySecretFuture = secretOperations.getAsync(config.getCaNamespace(), config.getEoKeySecretName());
        } else {
            eoKeySecretFuture = Future.succeededFuture(null);
        }

        CompositeFuture.join(clusterCaCertSecretFuture, eoKeySecretFuture)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        Admin adminClient = adminClientProvider.createAdminClient(config.getKafkaBootstrapServers(),
                                clusterCaCertSecretFuture.result(), eoKeySecretFuture.result(), eoKeySecretFuture.result() != null ? "entity-operator" : null);
                        promise.complete(adminClient);
                    } else {
                        promise.fail(ar.cause());
                    }
                });

        return promise.future();
    }
}
