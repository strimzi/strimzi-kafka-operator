/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Represents the change from one Kafka version to another
 */
public class KafkaVersionChange {
    private final KafkaVersion from;
    private final KafkaVersion to;
    private final int compare;

    public KafkaVersionChange(KafkaVersion from, KafkaVersion to) {
        this.from = from;
        this.to = to;
        this.compare = from.compareTo(to);
    }

    /**
     * The version being changed from.
     * @return The version being changed from.
     */
    public KafkaVersion from() {
        return from;
    }

    /**
     * The version being changed to.
     * @return The version being changeed to.
     */
    public KafkaVersion to() {
        return to;
    }

    /**
     * true if changing Kafka from {@link #from()} to {@code to} requires changing the Zookeeper version.
     * @return true if changing Kafka from {@link #from()} to {@code to} requires upgrading the Zookeeper version.
     */
    public boolean requiresZookeeperChange() {
        return !from.zookeeperVersion().equals(to.zookeeperVersion());
    }

    /**
     * @return true if this is an upgrade.
     */
    public boolean isUpgrade() {
        return compare < 0;
    }

    /**
     * @return true if this is a downgrade.
     */
    public boolean isDowngrade() {
        return compare > 0;
    }

    /**
     * @return true if there is no version change.
     */
    public boolean isNoop() {
        return compare == 0;
    }

    @Override
    public String toString() {
        if (isUpgrade()) {
            return "Kafka version upgrade from " + from + " to " + to;
        } else if (isDowngrade()) {
            return "Kafka version downgrade from " + from + " to " + to;
        } else {
            return "Kafka version=" + from + " (no version change)";
        }
    }

}
