/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.operator.user.operator.KafkaUserQuotasOperator;
import io.strimzi.operator.user.operator.ScramShaCredentials;
import io.strimzi.operator.user.operator.ScramShaCredentialsOperator;
import io.strimzi.operator.user.operator.SimpleAclOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

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
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(options);
        KubernetesClient client = new DefaultKubernetesClient();
        kafka.security.auth.SimpleAclAuthorizer authorizer = createSimpleAclAuthorizer(config);

        run(vertx, client, authorizer, config).setHandler(ar -> {
            if (ar.failed()) {
                log.error("Unable to start operator", ar.cause());
                System.exit(1);
            }
        });
    }

    static Future<String> run(Vertx vertx, KubernetesClient client, kafka.security.auth.SimpleAclAuthorizer authorizer, UserOperatorConfig config) {
        printEnvInfo();
        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        OpenSslCertManager certManager = new OpenSslCertManager();
        SecretOperator secretOperations = new SecretOperator(vertx, client);
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> crdOperations = new CrdOperator<>(vertx, client, KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
        SimpleAclOperator aclOperations = new SimpleAclOperator(vertx, authorizer);
        ScramShaCredentials scramShaCredentials = new ScramShaCredentials(config.getZookeperConnect(), (int) config.getZookeeperSessionTimeoutMs());
        ScramShaCredentialsOperator scramShaCredentialsOperator = new ScramShaCredentialsOperator(vertx, scramShaCredentials);
        KafkaUserQuotasOperator quotasOperator = new KafkaUserQuotasOperator(vertx, config.getZookeperConnect(), (int) config.getZookeeperSessionTimeoutMs());

        KafkaUserOperator kafkaUserOperations = new KafkaUserOperator(vertx,
                certManager, crdOperations,
                config.getLabels(),
                secretOperations, scramShaCredentialsOperator, quotasOperator, aclOperations, config.getCaCertSecretName(), config.getCaKeySecretName(), config.getCaNamespace());

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
    }

    private static kafka.security.auth.SimpleAclAuthorizer createSimpleAclAuthorizer(UserOperatorConfig config) {
        log.debug("Creating SimpleAclAuthorizer for Zookeeper {}", config.getZookeperConnect());
        Map<String, Object> authorizerConfig = new HashMap<>();
        // The SimpleAclAuthorizer from KAfka requires the Zookeeper URL to be provided twice.
        // See the comments in the SimpleAclAuthorizer.scala class for more details
        authorizerConfig.put(kafka.security.auth.SimpleAclAuthorizer.ZkUrlProp(), config.getZookeperConnect());
        authorizerConfig.put("zookeeper.connect", config.getZookeperConnect());
        authorizerConfig.put(kafka.security.auth.SimpleAclAuthorizer.ZkConnectionTimeOutProp(), config.getZookeeperSessionTimeoutMs());
        authorizerConfig.put(kafka.security.auth.SimpleAclAuthorizer.ZkSessionTimeOutProp(), config.getZookeeperSessionTimeoutMs());

        kafka.security.auth.SimpleAclAuthorizer authorizer = new kafka.security.auth.SimpleAclAuthorizer();
        authorizer.configure(authorizerConfig);
        return authorizer;
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
