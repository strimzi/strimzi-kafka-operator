/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

/**
 * Context passed to the exit methods of the KafkaConnect plugins (both mutating and validating). It is invoked after a KafkaConnect
 * reconciliation completes.
 */
public interface GatekeeperKafkaConnectExitContext { }
