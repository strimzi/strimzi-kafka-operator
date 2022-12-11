/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.operator.resource.StatusUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Checks for potential problems in the configuration requested by the user, to provide
 * warnings and share best practice. The intent is this class will generate warnings about
 * configurations that aren't necessarily illegal or invalid, but that could potentially
 * lead to problems.
 */
public class ZooKeeperSpecChecker {
    private final ZookeeperCluster zk;

    /**
     * @param zk The model generated based on the spec. This is requested so that default
     *                     values not included in the spec can be taken into account, without needing
     *                     this class to include awareness of what defaults are applied.
     */
    public ZooKeeperSpecChecker(ZookeeperCluster zk) {
        this.zk = zk;
    }

    /**
     * Runs the spec checker
     *
     * @return  List of warning conditions
     */
    public List<Condition> run() {
        List<Condition> warnings = new ArrayList<>();
        checkZooKeeperStorage(warnings);
        checkZooKeeperReplicas(warnings);
        return warnings;
    }

    /**
     * Checks for a single-node ZooKeeper cluster using ephemeral storage. This is potentially a problem as it
     * means any restarts of the pod will cause the loss of cluster metadata.
     *
     * @param warnings List to add a warning to, if appropriate.
     */
    private void checkZooKeeperStorage(List<Condition> warnings) {
        if (zk.getReplicas() == 1 && StorageUtils.usesEphemeral(zk.getStorage())) {
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
        if (zk.getReplicas() == 2) {
            warnings.add(StatusUtils.buildWarningCondition("ZooKeeperReplicas",
                    "Running ZooKeeper with two nodes is not advisable as both replicas will be needed to avoid downtime. It is recommended that a minimum of three replicas are used."));
        } else if (zk.getReplicas() % 2 == 0) {
            warnings.add(StatusUtils.buildWarningCondition("ZooKeeperReplicas",
                    "Running ZooKeeper with an odd number of replicas is recommended."));
        }
    }

}
