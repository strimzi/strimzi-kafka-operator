/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.AbstractJsonDiff;
import io.strimzi.operator.common.model.OrderedProperties;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The algorithm:
 *  1. Create a map from the supplied desired String
 *  2. Fill placeholders (e.g. ${BROKER_ID}) in desired map as the broker's {@code kafka_config_generator.sh} would
 *  3a. Loop over all entries. If the entry is in IGNORABLE_PROPERTIES or entry.value from desired is equal to entry.value from current, do nothing
 *      else add it to the diff
 *  3b. If entry was removed from desired, add it to the diff with null value.
 *  3c. If custom entry was removed, delete property
 */
public class KafkaNodeConfigurationDiff extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaNodeConfigurationDiff.class);

    /**
     * These options are skipped because they contain placeholders
     * 909[1-4] is for skipping all (internal, plain, secured, external) listeners properties
     */
    public static final Pattern IGNORABLE_PROPERTIES = Pattern.compile(
            "^(broker\\.id"
            + "|.*-[0-9]{2,5}\\.ssl\\.keystore\\.location"
            + "|.*-[0-9]{2,5}\\.ssl\\.keystore\\.password"
            + "|.*-[0-9]{2,5}\\.ssl\\.keystore\\.type"
            + "|.*-[0-9]{2,5}\\.ssl\\.truststore\\.location"
            + "|.*-[0-9]{2,5}\\.ssl\\.truststore\\.password"
            + "|.*-[0-9]{2,5}\\.ssl\\.truststore\\.type"
            + "|.*-[0-9]{2,5}\\.ssl\\.client\\.auth"
            + "|.*-[0-9]{2,5}\\.scram-sha-512\\.sasl\\.jaas\\.config"
            + "|.*-[0-9]{2,5}\\.sasl\\.enabled\\.mechanisms"
            + "|advertised\\.listeners"
            + "|zookeeper\\.connect"
            + "|zookeeper\\.ssl\\..*"
            + "|zookeeper\\.clientCnxnSocket"
            + "|broker\\.rack)$");

    /**
     * KRaft controller configuration options are skipped if it is not combined node
     */
    private static final Pattern IGNORABLE_CONTROLLER_PROPERTIES = Pattern.compile("controller\\.quorum\\..*");

    /**
     * List of configuration options that are relevant to controllers and should be considered when deciding whether
     * a controller-only node needs to be rolled or not.
     * The options that contain placeholders
     * 9090 is for skipping all (internal, plain, secured, external) listeners properties
     */
    private static final Set<String> CONTROLLER_RELEVANT_CONFIGS = Set.of(
            "alter.config.policy.class.name",
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
            "default.replication.factor",
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
            "num.io.threads",
            "num.network.threads",
            "num.partitions",
            "offsets.topic.replication.factor",
            "principal.builder.class",
            "process.roles",
            "replica.selector.class",
            "reserved.broker.max.id",
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
            "super.users",
            "transaction.state.log.min.isr",
            "transaction.state.log.replication.factor",
            "queued.max.requests",
            "queued.max.requests.bytes",
            "unclean.leader.election.enable"
    );

    private final Reconciliation reconciliation;
    private final Collection<AlterConfigOp> nodeConfigDiff;
    private final Map<String, ConfigModel> configModel;
    private final boolean isController;
    private final boolean isBroker;

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param nodeConfigs       Kafka node configuration from Kafka Admin API
     * @param desired           Desired configuration
     * @param kafkaVersion      Kafka version
     * @param nodeRef           Node reference
     */
    protected KafkaNodeConfigurationDiff(Reconciliation reconciliation, Config nodeConfigs, String desired, KafkaVersion kafkaVersion, NodeRef nodeRef, boolean isController, boolean isBroker) {
        this.reconciliation = reconciliation;
        this.configModel = KafkaConfiguration.readConfigModel(kafkaVersion);
        this.isController = isController;
        this.isBroker = isBroker;
        this.nodeConfigDiff = diff(nodeRef, desired, nodeConfigs, configModel);

    }

    /**
     * @return  Returns true if the configuration can be updated dynamically
     */
    protected boolean canBeUpdatedDynamically() {
        boolean result = true;
        for (AlterConfigOp entry : nodeConfigDiff) {
            if (isEntryReadOnly(entry.configEntry())) {
                result = false;
                LOGGER.infoCr(reconciliation, "Configuration can't be updated dynamically due to: {}", entry);
                break;
            }
        }
        return result;
    }

    /**
     * @param entry tested ConfigEntry
     * @return true if the entry is READ_ONLY
     */
    private boolean isEntryReadOnly(ConfigEntry entry) {
        return configModel.get(entry.name()).getScope().equals(Scope.READ_ONLY);
    }

    /**
     * Returns configuration difference
     * @return Collection of AlterConfigOp containing difference between current and desired configuration
     */
    protected Collection<AlterConfigOp> getConfigDiff() {
        return nodeConfigDiff;
    }

    /**
     * @return The number of broker configs which are different.
     */
    protected int getDiffSize() {
        return nodeConfigDiff.size();
    }

    private boolean isIgnorableProperty(final String key) {
        if (isController && !isBroker) {
            // If this config is not relevant to controllers, ignore it for a pure controller
            // ignorable properties are not included in the controller relevant configs so no need to check it as well
            return !CONTROLLER_RELEVANT_CONFIGS.contains(key);
        } else if (isController) {
            return IGNORABLE_PROPERTIES.matcher(key).matches();
        } else {
            // If node is not a KRaft controller, ignore KRaft controller config properties.
            return IGNORABLE_PROPERTIES.matcher(key).matches() || IGNORABLE_CONTROLLER_PROPERTIES.matcher(key).matches();
        }
    }

    /**
     * Computes diff between two maps. Entries in IGNORABLE_PROPERTIES are skipped
     * @param nodeRef node reference of compared node
     * @param desired desired configuration, may be null if the related ConfigMap does not exist yet or no changes are required
     * @param nodeConfigs current configuration
     * @param configModel default configuration for {@code kafkaVersion} of broker
     * @return Collection of AlterConfigOp containing all entries which were changed from current in desired configuration
     */
    private Collection<AlterConfigOp> diff(NodeRef nodeRef, String desired,
                                                  Config nodeConfigs,
                                                  Map<String, ConfigModel> configModel) {
        if (nodeConfigs == null || desired == null) {
            return Collections.emptyList();
        }
        Map<String, String> currentMap;

        Collection<AlterConfigOp> updatedCE = new ArrayList<>();

        currentMap = nodeConfigs.entries().stream().collect(
            Collectors.toMap(
                ConfigEntry::name,
                configEntry -> configEntry.value() == null ? "null" : configEntry.value()));

        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredMap = orderedProperties.asMap();

        JsonNode source = PATCH_MAPPER.valueToTree(currentMap);
        JsonNode target = PATCH_MAPPER.valueToTree(desiredMap);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);

        for (JsonNode d : jsonDiff) {
            String pathValue = d.get("path").asText();
            String pathValueWithoutSlash = pathValue.substring(1);

            Optional<ConfigEntry> optEntry = nodeConfigs.entries().stream()
                    .filter(configEntry -> configEntry.name().equals(pathValueWithoutSlash))
                    .findFirst();

            String op = d.get("op").asText();
            if (optEntry.isPresent()) {
                ConfigEntry entry = optEntry.get();
                if ("remove".equals(op)) {
                    removeProperty(configModel, updatedCE, pathValueWithoutSlash, entry);
                } else if ("replace".equals(op)) {
                    // entry is in the current, desired is updated value
                    updateOrAdd(entry.name(), configModel, desiredMap, updatedCE);
                }
            } else {
                if ("add".equals(op)) {
                    // entry is not in the current, it is added
                    updateOrAdd(pathValueWithoutSlash, configModel, desiredMap, updatedCE);
                }
            }

            if ("remove".equals(op)) {
                // there is a lot of properties set by default - not having them in desired causes very noisy log output
                LOGGER.traceCr(reconciliation, "Kafka Broker {} Config Differs : {}", nodeRef.nodeId(), d);
                LOGGER.traceCr(reconciliation, "Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
                LOGGER.traceCr(reconciliation, "Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
            } else {
                LOGGER.debugCr(reconciliation, "Kafka Broker {} Config Differs : {}", nodeRef.nodeId(), d);
                LOGGER.debugCr(reconciliation, "Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
                LOGGER.debugCr(reconciliation, "Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
            }
        }

        return updatedCE;
    }

    private void updateOrAdd(String propertyName, Map<String, ConfigModel> configModel, Map<String, String> desiredMap, Collection<AlterConfigOp> updatedCE) {
        if (!isIgnorableProperty(propertyName)) {
            if (KafkaConfiguration.isCustomConfigurationOption(propertyName, configModel)) {
                LOGGER.traceCr(reconciliation, "custom property {} has been updated/added {}", propertyName, desiredMap.get(propertyName));
            } else {
                LOGGER.traceCr(reconciliation, "property {} has been updated/added {}", propertyName, desiredMap.get(propertyName));
                updatedCE.add(new AlterConfigOp(new ConfigEntry(propertyName, desiredMap.get(propertyName)), AlterConfigOp.OpType.SET));
            }
        } else {
            LOGGER.traceCr(reconciliation, "{} is ignorable, not considering");
        }
    }

    private void removeProperty(Map<String, ConfigModel> configModel, Collection<AlterConfigOp> updatedCE, String pathValueWithoutSlash, ConfigEntry entry) {
        if (KafkaConfiguration.isCustomConfigurationOption(entry.name(), configModel)) {
            // we are deleting custom option
            LOGGER.traceCr(reconciliation, "removing custom property {}", entry.name());
        } else if (entry.isDefault()) {
            // entry is in current, is not in desired, is default -> it uses default value, skip.
            // Some default properties do not have set ConfigEntry.ConfigSource.DEFAULT_CONFIG and thus
            // we are removing property. That might cause redundant RU. To fix this we would have to add defaultValue
            // to the configModel
            LOGGER.traceCr(reconciliation, "{} not set in desired, using default value", entry.name());
        } else {
            // entry is in current, is not in desired, is not default -> it was using non-default value and was removed
            // if the entry was custom, it should be deleted
            if (!isIgnorableProperty(pathValueWithoutSlash)) {
                updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, null), AlterConfigOp.OpType.DELETE));
                LOGGER.infoCr(reconciliation, "{} not set in desired, unsetting back to default {}", entry.name(), "deleted entry");
            } else {
                LOGGER.traceCr(reconciliation, "{} is ignorable, not considering as removed");
            }
        }
    }

    /**
     * @return whether the current config and the desired config are identical (thus, no update is necessary).
     */
    @Override
    public boolean isEmpty() {
        return nodeConfigDiff.isEmpty();
    }
}
