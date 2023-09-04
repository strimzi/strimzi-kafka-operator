/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.model.AbstractJsonDiff;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 The algorithm:
 *  1. Create a map from the supplied current Config.
 *  2. Create a map from the supplied desired String.
 *  3. Generate JSON diff from the maps.
 *  4a. If entry was added, replaced or moved from desired, check whether the entry is in CONTROLLER_CONFIGS.
 *  4b. If the entry in CONTROLLER_CONFIGS, set <code>configsHaveChanged</code> to true.
 *  4c. If the entry is not in CONTROLLER_CONFIGS, set <code>configsHaveChanged</code> to false.
 */
public class KafkaControllerConfigurationDiff extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaControllerConfigurationDiff.class);
    private final Reconciliation reconciliation;
    final boolean configsHaveChanged;

    KafkaControllerConfigurationDiff(Reconciliation reconciliation, Config controllerConfig, String desired, int brokerId) {
        this.reconciliation = reconciliation;
        this.configsHaveChanged = configsHaveChanged(brokerId, desired, controllerConfig);
    }

    /**
     * Computes diff between two maps. Checks if entries are in CONTROLLER_CONFIGS.
     * @param brokerId id of compared broker
     * @param desired desired configuration, may be null if the related ConfigMap does not exist yet or no changes are required.
     * @param controllerConfig current configuration.
     * @return Returns true if changed entries from desired configurations are in CONTROLLER_CONFIGS, otherwise false.
     */
    private boolean configsHaveChanged(int brokerId, String desired, Config controllerConfig) {
        if (controllerConfig == null || desired == null) {
            return false;
        }
        Map<String, String> current;

        current = controllerConfig.entries().stream().collect(
                Collectors.toMap(
                        ConfigEntry::name,
                        configEntry -> String.valueOf(configEntry.value())));
        LOGGER.traceCr(reconciliation, "Kafka KRaft Controller {} Current Config {}", brokerId, current);

        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredConfig = orderedProperties.asMap();
        LOGGER.traceCr(reconciliation, "Kafka KRaft Controller {} Desired Config {}", brokerId, desiredConfig);

        JsonNode source = PATCH_MAPPER.valueToTree(current);
        JsonNode target = PATCH_MAPPER.valueToTree(desiredConfig);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);
        LOGGER.traceCr(reconciliation, "Kafka KRaft Controller {} Config Diff {}", brokerId, jsonDiff);

        boolean configsHaveChanged = false;
        for (JsonNode node : jsonDiff) {
            String configPath = node.get("path").asText();
            if (ControllerConfigs.isControllerConfig(node.get("path").asText().substring(1))) {
                LOGGER.debugCr(reconciliation, "Kafka KRaft Controller {} Config Differs : {}", brokerId, configPath);
                LOGGER.debugCr(reconciliation, "Current Kafka KRaft Controller Config path {} has value {}", configPath, lookupPath(source, configPath));
                LOGGER.debugCr(reconciliation, "Desired Kafka KRaft Controller Config path {} has value {}", configPath, lookupPath(target, configPath));
                configsHaveChanged = true;
            }
        }

        return configsHaveChanged;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    static class ControllerConfigs {
        static final List<String> CONTROLLER_CONFIGS = List.of(
                "alter.config.policy.class.name",
                "authorizer.class.name",
                "background.threads",
                "broker.heartbeat.interval.ms",
                "broker.session.timeout.ms",
                "connection.failed.authentication.delay.ms",
                "connections.max.idle.ms",
                "connections.max.reauth.ms",
                "controlled.shutdown.enable",
                "controlled.shutdown.max.retries",
                "controlled.shutdown.retry.backoff.ms",
                "controller.listener.names",
                "controller.quorum.append.linger.ms",
                "controller.quorum.election.backoff.max.ms",
                "controller.quorum.election.timeout.ms",
                "controller.quorum.fetch.timeout.ms",
                "controller.quorum.request.timeout.ms",
                "controller.quorum.retry.backoff.ms",
                "controller.quorum.voters",
                "controller.quota.window.num",
                "controller.quota.window.size.seconds",
                "controller.socket.timeout.ms",
                "create.topic.policy.class.name",
                "delegation.token.master.key",
                "delegation.token.secret.key",
                "delegation.token.max.lifetime.ms",
                "delegation.token.expiry.time.ms",
                "delegation.token.expiry.check.interval.ms",
                "delete.topic.enable",
                "early.start.listeners",
                "kafka.metrics.polling.interval.secs",
                "kafka.metrics.reporters",
                "leader.imbalance.check.interval.seconds",
                "leader.imbalance.per.broker.percentage",
                "listener.security.protocol.map",
                "listeners",
                "log.dir",
                "log.dirs",
                "max.connection.creation.rate",
                "max.connections.per.ip.overrides",
                "max.connections.per.ip",
                "max.connections",
                "metadata.log.dir",
                "metadata.log.max.record.bytes.between.snapshots",
                "metadata.log.max.snapshot.interval.ms",
                "metadata.log.segment.bytes",
                "metadata.log.segment.min.bytes",
                "metadata.log.segment.ms",
                "metadata.max.idle.interval.ms",
                "metadata.max.retention.bytes",
                "metadata.max.retention.ms",
                "metric.reporters",
                "metrics.num.samples",
                "metrics.recording.level",
                "metrics.sample.window.ms",
                "node.id",
                "num.io.threads",
                "num.network.threads",
                "principal.builder.class",
                "process.roles",
                "queued.max.requests",
                "queued.max.request.bytes",
                "sasl.enabled.mechanisms",
                "sasl.kerberos.kinit.cmd",
                "sasl.kerberos.min.time.before.relogin",
                "sasl.kerberos.principal.to.local.rules",
                "sasl.kerberos.service.name",
                "sasl.kerberos.ticket.renew.jitter",
                "sasl.kerberos.ticket.renew.window.factor",
                "sasl.login.callback.handler.class",
                "sasl.login.class",
                "sasl.login.connect.timeout.ms",
                "sasl.login.read.timeout.ms",
                "sasl.login.refresh.buffer.seconds",
                "sasl.login.refresh.min.period.seconds",
                "sasl.login.refresh.window.factor",
                "sasl.login.refresh.window.jitter",
                "sasl.login.retry.backoff.max.ms",
                "sasl.login.retry.backoff.ms",
                "sasl.mechanism.controller.protocol",
                "sasl.oauthbearer.clock.skew.seconds",
                "sasl.oauthbearer.expected.audience",
                "sasl.oauthbearer.expected.issuer",
                "sasl.oauthbearer.jwks.endpoint.refresh.ms",
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms",
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms",
                "sasl.oauthbearer.jwks.endpoint.url",
                "sasl.oauthbearer.scope.claim.name",
                "sasl.oauthbearer.sub.claim.name",
                "sasl.oauthbearer.token.endpoint.url",
                "sasl.server.callback.handler.class",
                "sasl.server.max.receive.size",
                "security.providers",
                "server.max.startup.time.ms",
                "socket.connection.setup.timeout.max.ms",
                "socket.connection.setup.timeout.ms",
                "socket.listen.backlog.size",
                "socket.receive.buffer.bytes",
                "socket.request.max.bytes",
                "socket.send.buffer.bytes",
                "ssl.cipher.suites",
                "ssl.client.auth",
                "ssl.enabled.protocols",
                "ssl.endpoint.identification.algorithm",
                "ssl.engine.factory.class",
                "ssl.key.password",
                "ssl.keymanager.algorithm",
                "ssl.keystore.certificate.chain",
                "ssl.keystore.key",
                "ssl.keystore.location",
                "ssl.keystore.password",
                "ssl.keystore.type",
                "ssl.principal.mapping.rules",
                "ssl.protocol",
                "ssl.provider",
                "ssl.secure.random.implementation",
                "ssl.trustmanager.algorithm",
                "ssl.truststore.certificates",
                "ssl.truststore.location",
                "ssl.truststore.password",
                "ssl.truststore.type"
        );

        static boolean isControllerConfig(String config) {
            return CONTROLLER_CONFIGS.contains(config);
        }
    }
}
