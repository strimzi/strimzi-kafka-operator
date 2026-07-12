/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

/**
 * Context passed to the deletion hook of the KafkaConnect plugins. It is invoked when a KafkaConnect is being deleted.
 */
public interface GatekeeperKafkaConnectDeletionContext { }
