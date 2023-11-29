/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.vertx.core.Future;

/**
 * Interface for creating the Version Change that stores the various versions that should be used. This has two
 * implementations - one for KRaft and one for ZooKeeper.
 */
public interface VersionChangeCreator {
    /**
     * @return  Creates the KafkaVersionChange instance based on the Kafka CR and the current state of the environment
     */
    Future<KafkaVersionChange> reconcile();
}
