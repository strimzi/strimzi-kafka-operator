/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Represents the upgrade or downgrade from one Kafka version to another
 */
public class KafkaUpgrade {
    private final KafkaVersion from;
    private final KafkaVersion to;
    private final int compare;

    public KafkaUpgrade(KafkaVersion from, KafkaVersion to) {
        this.from = from;
        this.to = to;
        this.compare = from.compareTo(to);
    }

    /** The version being upgraded from. */
    public KafkaVersion from() {
        return from;
    }

    /** The version being upgraded to. */
    public KafkaVersion to() {
        return to;
    }

    /** true if upgrading from {@link #from()} to {@code to} requires upgrading the inter broker protocol. */
    public boolean requiresProtocolChange() {
        return !from.protocolVersion().equals(to.protocolVersion());
    }

    /** true if upgrading from {@link #from()} to {@code to} requires upgrading the message format. */
    public boolean requiresMessageFormatChange() {
        return !from.messageVersion().equals(to.messageVersion());
    }

    public boolean isUpgrade() {
        return compare < 0;
    }

    public boolean isDowngrade() {
        return compare > 0;
    }

    public boolean isNoop() {
        return compare == 0;
    }

    public String description() {
        if (isUpgrade()) {
            return "upgrade";
        } else if (isDowngrade()) {
            return "downgrade";
        } else {
            return "no version change";
        }
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
