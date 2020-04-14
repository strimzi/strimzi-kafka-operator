/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.operator.resource.StatusUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Checks for potential problems in the configuration requested by the user, to provide
 * warnings and share best practice. The intent is this class will generate warnings about
 * configurations that aren't necessarily illegal or invalid, but that could potentially
 * lead to problems.
 */
public class KafkaSpecChecker {

    private KafkaSpec spec;
    private KafkaCluster kafkaCluster;
    private ZookeeperCluster zkCluster;

    private final static Pattern VERSION_REGEX = Pattern.compile("(\\d\\.\\d+).*");

    /**
     * @param spec The spec requested by the user in the CR
     * @param kafkaCluster The model generated based on the spec. This is requested so that default
     *                     values not included in the spec can be taken into account, without needing
     *                     this class to include awareness of what defaults are applied.
     * @param zkCluster The model generated based on the spec. This is requested so that default
     *                     values not included in the spec can be taken into account, without needing
     *                     this class to include awareness of what defaults are applied.
     */
    public KafkaSpecChecker(KafkaSpec spec, KafkaCluster kafkaCluster, ZookeeperCluster zkCluster) {
        this.spec = spec;
        this.kafkaCluster = kafkaCluster;
        this.zkCluster = zkCluster;
    }

    public List<Condition> run() {
        List<Condition> warnings = new ArrayList<>();
        checkKafkaLogMessageFormatVersion(warnings);
        checkKafkaStorage(warnings);
        checkZooKeeperStorage(warnings);
        checkZooKeeperReplicas(warnings);
        return warnings;
    }

    /**
     * Checks if the version of the Kafka brokers matches any custom log.message.format.version config.
     *
     * Updating this is the final step in upgrading Kafka version, so if this doesn't match it is possibly an
     * indication that a user has updated their Kafka cluster and is unaware that they also should update
     * their format version to match.
     *
     * @param warnings List to add a warning to, if appropriate.
     */
    private void checkKafkaLogMessageFormatVersion(List<Condition> warnings) {
        String logMsgFormatVersion = kafkaCluster.getConfiguration().getConfigOption(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION);
        String kafkaBrokerVersion = spec.getKafka().getVersion();
        if (logMsgFormatVersion != null && kafkaBrokerVersion != null) {
            Matcher m = VERSION_REGEX.matcher(logMsgFormatVersion);
            if (m.find() && !kafkaBrokerVersion.startsWith(m.group(0))) {
                warnings.add(StatusUtils.buildWarningCondition("KafkaLogMessageFormatVersion",
                        "log.message.format.version does not match the Kafka cluster version, which suggests that an upgrade is incomplete."));
            }
        }
    }

    /**
     * Checks for a single-broker Kafka cluster using ephemeral storage. This is potentially a problem as it
     * means any restarts of the broker will result in data loss, as the single broker won't allow for any
     * topic replicas.
     *
     * @param warnings List to add a warning to, if appropriate.
     */
    private void checkKafkaStorage(List<Condition> warnings) {
        if (kafkaCluster.getReplicas() == 1 && StorageUtils.usesEphemeral(kafkaCluster.getStorage())) {
            warnings.add(StatusUtils.buildWarningCondition("KafkaStorage",
                    "A Kafka cluster with a single replica and ephemeral storage will lose topic messages after any restart or rolling update."));
        }
    }

    /**
     * Checks for a single-node ZooKeeper cluster using ephemeral storage. This is potentially a problem as it
     * means any restarts of the pod will cause the loss of cluster metadata.
     *
     * @param warnings List to add a warning to, if appropriate.
     */
    private void checkZooKeeperStorage(List<Condition> warnings) {
        if (zkCluster.getReplicas() == 1 && StorageUtils.usesEphemeral(zkCluster.getStorage())) {
            warnings.add(StatusUtils.buildWarningCondition("ZooKeeperStorage",
                    "A ZooKeeper cluster with a single replica and ephemeral storage will be in a defective state after any restart or rolling update. It is recommended that a minimum of three replicas are used."));
        }
    }

    /**
     * Checks for an even number of ZooKeeper replicas. As ZooKeeper is dependent on maintaining a quorum,
     * this means that users should deploy clusters with an odd number of nodes.
     *
     * @param warnings List to add a warning to, if appropriate.
     */
    private void checkZooKeeperReplicas(List<Condition> warnings) {
        if (zkCluster.getReplicas() == 2) {
            warnings.add(StatusUtils.buildWarningCondition("ZooKeeperReplicas",
                    "Running ZooKeeper with two nodes is not advisable as both replicas will be needed to avoid downtime. It is recommended that a minimum of three replicas are used."));
        } else if (zkCluster.getReplicas() % 2 == 0) {
            warnings.add(StatusUtils.buildWarningCondition("ZooKeeperReplicas",
                    "Running ZooKeeper with an odd number of replicas is recommended."));
        }
    }

}
