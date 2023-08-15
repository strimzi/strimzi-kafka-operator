/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.AbstractJsonDiff;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaControllerConfigurationDiff extends AbstractJsonDiff {
    final boolean configsHaveChanged;

    protected KafkaControllerConfigurationDiff(Config controllerConfig, String desired) {
        this.configsHaveChanged = configsHaveChanged(desired, controllerConfig);
    }

    private boolean configsHaveChanged(String desired, Config controllerConfig) {
        Map<String, String> current;

        current = controllerConfig.entries().stream().collect(
                Collectors.toMap(
                        ConfigEntry::name,
                        configEntry -> String.valueOf(configEntry.value())));

        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredConfig = orderedProperties.asMap();

        JsonNode source = PATCH_MAPPER.valueToTree(current);
        JsonNode target = PATCH_MAPPER.valueToTree(desiredConfig);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);

        for (JsonNode node : jsonDiff) {
            String operation = node.get("op").asText();
            if ("replace".equals(operation) || "move".equals(operation) || "add".equals(operation)) {
                return ControllerConfigs.isControllerConfig(node.get("path").asText().substring(1));
            }
        }

        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    static class ControllerConfigs {
        static final List<String> CONTROLLER_CONFIGS = List.of(
                "authorizer.class.name",
                "auto.create.topics.enable",
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
                "delete.topic.enable",
                "early.start.listeners",
                "initial.broker.registration.timeout.ms",
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
                "ssl.truststore.type",
                "unclean.leader.election.enable"
        );

        static boolean isControllerConfig(String config) {
            return CONTROLLER_CONFIGS.contains(config);
        }
    }
}
