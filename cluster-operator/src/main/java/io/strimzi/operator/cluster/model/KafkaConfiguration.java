/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.operator.common.Reconciliation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {
    /**
     * Configuration key of the inter-broker protocol version option
     */
    public static final String INTERBROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";

    /**
     * Configuration key of the message format version option
     */
    public static final String LOG_MESSAGE_FORMAT_VERSION = "log.message.format.version";

    /**
     * Configuration key of the default replication factor option
     */
    public static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";

    /**
     * Configuration key of the min-insync replicas version option
     */
    public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";

    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS;

    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesToList(KafkaClusterSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesToList(KafkaClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    /**
     * List of configuration options that are relevant to controllers and should be considered when deciding whether
     * a controller-only node needs to be rolled or not.
     */
    private static final Set<String> CONTROLLER_RELEVANT_CONFIGS = Set.of(
            "alter.config.policy.class.name",
            "authorizer.class.name",
            "auto.create.topics.enable",
            "background.threads",
            "broker.heartbeat.interval.ms",
            "broker.rack",
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
            "default.replication.factor",
            "delete.topic.enable",
            "early.start.listeners",
            "kafka.metrics.polling.interval.secs",
            "kafka.metrics.reporters",
            "leader.imbalance.check.interval.seconds",
            "leader.imbalance.per.broker.percentage",
            "listener.name.controlplane-9090.ssl.keystore.location",
            "listener.name.controlplane-9090.ssl.keystore.password",
            "listener.name.controlplane-9090.ssl.keystore.type",
            "listener.name.controlplane-9090.ssl.truststore.location",
            "listener.name.controlplane-9090.ssl.truststore.password",
            "listener.name.controlplane-9090.ssl.truststore.type",
            "listener.name.controlplane-9090.ssl.client.auth",
            "listener.security.protocol.map",
            "listeners",
            "log.dir",
            "log.dirs",
            "min.insync.replicas",
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
            "offsets.topic.replication.factor",
            "principal.builder.class",
            "process.roles",
            "replica.selector.class",
            "reserved.broker.max.id",
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
            "transaction.state.log.min.isr",
            "transaction.state.log.replication.factor",
            "queued.max.requests",
            "queued.max.requests.bytes",
            "unclean.leader.election.enable"
    );

    /**
     * Copy constructor which creates new instance of the Kafka Configuration from existing configuration. It is
     * useful when you need to modify an instance of the configuration without permanently changing the original.
     *
     * @param configuration     Existing configuration
     */
    public KafkaConfiguration(KafkaConfiguration configuration)   {
        super(configuration);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    private KafkaConfiguration(Reconciliation reconciliation, String configuration, List<String> forbiddenPrefixes) {
        super(reconciliation, configuration, forbiddenPrefixes);
    }


    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     *
     * @param reconciliation The reconciliation
     * @param string A string representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaConfiguration unvalidated(Reconciliation reconciliation, String string) {
        return new KafkaConfiguration(reconciliation, string, emptyList());
    }

    /**
     * Validate the configs in this KafkaConfiguration returning a list of errors.
     * @param kafkaVersion The broker version.
     * @return A list of error messages.
     */
    public List<String> validate(KafkaVersion kafkaVersion) {
        List<String> errors = new ArrayList<>();
        Map<String, ConfigModel> models = readConfigModel(kafkaVersion);
        for (Map.Entry<String, String> entry: asOrderedProperties().asMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            ConfigModel config = models.get(key);
            if (config != null) {
                // It's not an error if config _is_ null because extra configs
                // might be intended for plugins
                errors.addAll(config.validate(key, value));
            }
        }
        return errors;
    }

    /**
     * Gets the config model for the given version of the Kafka broker.
     * @param kafkaVersion The broker version.
     * @return The config model for that broker version.
     */
    public static Map<String, ConfigModel> readConfigModel(KafkaVersion kafkaVersion) {
        String name = "/kafka-" + kafkaVersion.version() + "-config-model.json";
        try {
            try (InputStream in = KafkaConfiguration.class.getResourceAsStream(name)) {
                if (in != null) {
                    ConfigModels configModels = new ObjectMapper().readValue(in, ConfigModels.class);
                    if (!kafkaVersion.version().equals(configModels.getVersion())) {
                        throw new RuntimeException("Incorrect version");
                    }
                    return configModels.getConfigs();
                } else {
                    // The configuration model does not exist
                    throw new RuntimeException("Configuration model " + name + " was not found");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from classpath resource " + name, e);
        }
    }

    /**
     * Return the config properties with their values in this KafkaConfiguration which are not known broker configs.
     * These might be consumed by broker plugins.
     * @param kafkaVersion The broker version.
     * @return The unknown configs.
     */
    public Set<String> unknownConfigsWithValues(KafkaVersion kafkaVersion) {
        Map<String, ConfigModel> configModel = readConfigModel(kafkaVersion);
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, String> e :this.asOrderedProperties().asMap().entrySet()) {
            if (!configModel.containsKey(e.getKey())) {
                result.add(e.getKey() + "=" + e.getValue());
            }
        }
        return result;
    }

    /**
     * Return the config properties with their values in this KafkaConfiguration which are known to be relevant for the
     * Kafka controller nodes.
     *
     * @return  The configuration options relevant for controllers
     */
    public Set<String> controllerConfigsWithValues() {
        Set<String> result = new HashSet<>();

        for (Map.Entry<String, String> e :this.asOrderedProperties().asMap().entrySet()) {
            if (CONTROLLER_RELEVANT_CONFIGS.contains(e.getKey())) {
                result.add(e.getKey() + "=" + e.getValue());
            }
        }

        return result;
    }

    /**
     * @return  True if the configuration is empty. False otherwise.
     */
    public boolean isEmpty() {
        return this.asOrderedProperties().asMap().size() == 0;
    }
}
