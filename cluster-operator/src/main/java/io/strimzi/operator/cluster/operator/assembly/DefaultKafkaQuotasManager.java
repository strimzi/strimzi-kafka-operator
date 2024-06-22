/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPlugin;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class containing methods for handling the configuration around {@link QuotasPluginKafka}
 */
public class DefaultKafkaQuotasManager {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(DefaultKafkaQuotasManager.class.getName());

    /**
     * {@link ClientQuotaEntity} for the default users entity
     * When `null` is set for the ClientQuotaEntity with type USER, the default quotas for all users are used/configured
     */
    private static final ClientQuotaEntity DEFAULT_USER_ENTITY = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, null));
    private static final String PRODUCER_BYTE_RATE_QUOTA = "producer_byte_rate";
    private static final String CONSUMER_BYTE_RATE_QUOTA = "consumer_byte_rate";
    private static final String REQUEST_PERCENTAGE_QUOTA = "request_percentage";
    private static final String CONTROLLER_MUTATION_RATE_QUOTA = "controller_mutation_rate";

    /**
     * Returns empty instance of the {@link QuotasPluginKafka}
     * This is used in case when we need to reset the default Kafka quotas
     *
     * @return  empty instance of the {@link QuotasPluginKafka}
     */
    /* test */ static QuotasPluginKafka emptyQuotasPluginKafka() {
        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafka();

        quotasPluginKafka.setConsumerByteRate(null);
        quotasPluginKafka.setControllerMutationRate(null);
        quotasPluginKafka.setProducerByteRate(null);
        quotasPluginKafka.setRequestPercentage(null);

        return quotasPluginKafka;
    }

    /**
     * Creates list of {@link ClientQuotaAlteration.Op} based on the configuration in {@link QuotasPluginKafka}.
     * This list will be then passed to the Admin client and the default user quotas will be set.
     *
     * @param quotasPluginKafka     configuration of the quotas
     *
     * @return  list of {@link ClientQuotaAlteration.Op}
     */
    /* test */ static List<ClientQuotaAlteration.Op> prepareQuotaConfigurationRequest(QuotasPluginKafka quotasPluginKafka) {
        List<ClientQuotaAlteration.Op> ops = new ArrayList<>();

        ops.add(new ClientQuotaAlteration.Op(PRODUCER_BYTE_RATE_QUOTA,
            quotasPluginKafka.getProducerByteRate() != null ? Double.valueOf(quotasPluginKafka.getProducerByteRate()) : null));
        ops.add(new ClientQuotaAlteration.Op(CONSUMER_BYTE_RATE_QUOTA,
            quotasPluginKafka.getConsumerByteRate() != null ? Double.valueOf(quotasPluginKafka.getConsumerByteRate()) : null));
        ops.add(new ClientQuotaAlteration.Op(REQUEST_PERCENTAGE_QUOTA,
            quotasPluginKafka.getRequestPercentage() != null ? Double.valueOf(quotasPluginKafka.getRequestPercentage()) : null));
        ops.add(new ClientQuotaAlteration.Op(CONTROLLER_MUTATION_RATE_QUOTA,
            quotasPluginKafka.getControllerMutationRate() != null ? quotasPluginKafka.getControllerMutationRate() : null));

        return ops;
    }

    /**
     * Based on configuration in {@param quotasPlugin}, it configures the default user quota in Kafka.
     *
     * @param reconciliation            Reconciliation marker
     * @param vertx                     Vert.x instance
     * @param adminClientProvider       Kafka Admin client provider
     * @param pemTrustSet               Trust set for TLS authentication in PEM format
     * @param pemAuthIdentity           Identity for TLS client authentication in PEM format
     * @param quotasPlugin              Configuration of Kafka quotas plugin
     *
     * @return  Future that completes when the default user quota configuration is completed
     */
    public static Future<Void> reconcileDefaultUserQuotas(
        Reconciliation reconciliation,
        Vertx vertx,
        AdminClientProvider adminClientProvider,
        PemTrustSet pemTrustSet,
        PemAuthIdentity pemAuthIdentity,
        QuotasPlugin quotasPlugin
    ) {
        LOGGER.debugCr(reconciliation, "Reconciling default user quotas in Kafka");
        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;

        LOGGER.debugCr(reconciliation, "Creating AdminClient for setting default quota using {}", bootstrapHostname);
        Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, pemTrustSet, pemAuthIdentity);

        boolean isNotKafkaPlugin = !(quotasPlugin instanceof QuotasPluginKafka);
        QuotasPluginKafka quotasPluginKafka = isNotKafkaPlugin ? emptyQuotasPluginKafka() : (QuotasPluginKafka) quotasPlugin;

        List<ClientQuotaAlteration.Op> ops = prepareQuotaConfigurationRequest(quotasPluginKafka);

        return shouldAlterDefaultQuotasConfig(reconciliation, vertx, kafkaAdmin, ops, isNotKafkaPlugin)
            .compose(shouldUpdateQuotas -> {
                if (shouldUpdateQuotas) {
                    Promise<Void> promise = Promise.promise();

                    ClientQuotaAlteration clientQuotaAlteration = new ClientQuotaAlteration(DEFAULT_USER_ENTITY, ops);

                    LOGGER.debugCr(reconciliation, "Default user quotas differ and will be updated");
                    alterQuotas(reconciliation, vertx, kafkaAdmin, clientQuotaAlteration)
                        .onComplete(result -> {
                            if (result.succeeded()) {
                                LOGGER.debugCr(reconciliation, "Successfully altered default user quotas");
                                promise.complete();
                            } else {
                                LOGGER.errorCr(reconciliation, "Failed to alter default user quotas", result.cause());
                                promise.fail(result.cause());
                            }

                            kafkaAdmin.close();
                        });

                    return promise.future();
                } else {
                    kafkaAdmin.close();
                    return Future.succeededFuture();
                }
            });
    }

    /**
     * Checks whether default quotas configuration needs to be altered with new configuration.
     * It gets the current default quotas for users from Kafka and compares them with the desired configuration
     *
     * @param reconciliation          Reconciliation marker
     * @param vertx                   Vert.x instance
     * @param kafkaAdmin              Kafka Admin object
     * @param ops                     List of {@link ClientQuotaAlteration.Op}
     * @param isNotKafkaPlugin        boolean parameter determining if the Kafka built-in quotas plugin is used
     *
     * @return  result determining if the new quotas configuration should be applied or not
     */
    /* test */ static Future<Boolean> shouldAlterDefaultQuotasConfig(Reconciliation reconciliation, Vertx vertx, Admin kafkaAdmin, List<ClientQuotaAlteration.Op> ops, boolean isNotKafkaPlugin) {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation,
                vertx,
                kafkaAdmin.describeClientQuotas(ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER)))).entities())
            .compose(clientQuotaEntityMapMap -> {
                Map<String, Double> currentQuotas = clientQuotaEntityMapMap.get(DEFAULT_USER_ENTITY);

                if (currentQuotas == null) {
                    if (isNotKafkaPlugin) {
                        // 1. if the current quotas set in Kafka are null and quotas configuration in Kafka CR is not of type `QuotasPluginKafka`, skip alteration
                        LOGGER.debugCr(reconciliation, "There are no default user quotas set in Kafka and the Kafka built-in plugin is not configured, skipping the alteration");
                        return Future.succeededFuture(false);
                    } else if (ops.stream().allMatch(op -> Objects.isNull(op.value()))) {
                        // 2. if the current quotas set in Kafka are null and quotas configuration in Kafka CR is type of `QuotasPluginKafka`, but there is no fields configured, skip alteration
                        LOGGER.debugCr(reconciliation, "There are no default user quotas set in Kafka and no quotas are configured, skipping the alteration");
                        return Future.succeededFuture(false);
                    } else {
                        // 3. in case that the current quotas are null, but desired quotas contains some non-null values, we should alter the quotas
                        return Future.succeededFuture(true);
                    }
                } else {
                    return Future.succeededFuture(currentAndDesiredQuotasDiffer(currentQuotas, ops));
                }
            });
    }

    /**
     * Method for altering the default Kafka user quotas using Kafka Admin client
     *
     * @param reconciliation            Reconciliation marker
     * @param vertx                     Vert.x instance
     * @param kafkaAdmin                Kafka Admin object
     * @param clientQuotaAlteration     Quota alteration operation
     *
     * @return  Future after completion of the alter operation
     */
    private static Future<Void> alterQuotas(
        Reconciliation reconciliation,
        Vertx vertx,
        Admin kafkaAdmin,
        ClientQuotaAlteration clientQuotaAlteration
    ) {
        LOGGER.debugCr(reconciliation, "Altering default user quotas to: {}", clientQuotaAlteration.toString());

        return VertxUtil
            .kafkaFutureToVertxFuture(reconciliation, vertx, kafkaAdmin.alterClientQuotas(List.of(clientQuotaAlteration)).values().get(DEFAULT_USER_ENTITY))
            .map((Void) null);
    }

    /**
     * Method that compares the desired quotas, represented by List of {@link ClientQuotaAlteration.Op}, and current quotas
     * set in Kafka, represented by Map.
     *
     * @param desiredQuotas     desired quotas, represented as List of {@link ClientQuotaAlteration.Op}
     * @param currentQuotas     current quotas configured in Kafka, represented as Map
     *
     * @return  boolean result of the comparison. Returns true if both current and desired quotas are same, false otherwise
     */
    /* test */ static boolean currentAndDesiredQuotasDiffer(Map<String, Double> currentQuotas, List<ClientQuotaAlteration.Op> desiredQuotas) {
        // desiredQuotas will always contain all quotas keys, because it is filled from `prepareQuotaConfigurationRequest`
        // that's why we can iterate through the list and be sure that we cover all the quota keys
        for (ClientQuotaAlteration.Op quota : desiredQuotas) {
            Double currentValue = currentQuotas.get(quota.key());
            Double desiredValue = quota.value();

            if (!Objects.equals(currentValue, desiredValue)) {
                return true;
            }
        }

        return false;
    }
}
