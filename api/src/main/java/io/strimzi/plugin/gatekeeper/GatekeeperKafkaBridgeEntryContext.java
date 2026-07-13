/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

/**
 * Context passed to the entry methods of the KafkaBridge plugins (both mutating and validating). It is invoked at the start of
 * a KafkaBridge reconciliation.
 */
public interface GatekeeperKafkaBridgeEntryContext { }
