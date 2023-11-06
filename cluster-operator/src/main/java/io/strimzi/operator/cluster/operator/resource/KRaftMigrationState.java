/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Java representation of the JSON response from the /v1/kraft-migration endpoint of the KafkaAgent
 *
 * When a KRaft migration is going on and because it can take some time, the operator checks its status on each reconcile.
 * The /v1/kraft-migration endpoint of the KafkaAgent provides such information through a corresponding Kafka metric.
 */
public class KRaftMigrationState {

    /**
     * It's not possible to get the metric state through the KafkaAgent
     */
    public static final int UNKNOWN = -1;

    // coming from the ZkMigrationState enum in Apache Kafka upstream
    // https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/metadata/migration/ZkMigrationState.java
    /**
     * The cluster was created in KRaft mode.
     */
    public static final int NONE = 0;

    /**
     * The ZK data has been migrated, and the KRaft controller is now writing metadata to both ZK and the metadata log.
     */
    public static final int MIGRATION = 1;

    /**
     * KRaft controller quorum is deployed, waiting for brokers to register and start the migration.
     */
    public static final int PRE_MIGRATION = 2;

    /**
     * The migration from ZK has been fully completed.
     */
    public static final int POST_MIGRATION = 3;

    /**
     * The controller is a ZK controller. No migration has been performed.
     */
    public static final int ZK = 4;

    private final int state;

    /**
     * Constructor
     *
     * @param state state value
     */
    @JsonCreator
    public KRaftMigrationState(@JsonProperty("state") int state) {
        this.state = state;
    }

    /**
     * Integer that represents the ZooKeeper migration state, or -1 if there was an error when getting the ZooKeeper migration state.
     * @return integer result
     */
    public int state() {
        return state;
    }

    @Override
    public String toString() {
        return String.format("ZooKeeper migration state: %d", state);
    }

    /**
     * Returns true if the ZooKeeper migration state is 1 (MIGRATION) which means that migration is done
     * @return boolean result
     */
    public boolean isMigrationDone() {
        return state == MIGRATION;
    }
}
