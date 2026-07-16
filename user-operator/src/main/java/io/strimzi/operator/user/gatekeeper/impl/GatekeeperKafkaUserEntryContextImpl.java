/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.gatekeeper.impl;

import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserEntryContext;

/**
 * Default implementation of the {@link GatekeeperKafkaUserEntryContext} passed to the KafkaUser plugins at the start of
 * a KafkaUser reconciliation. It currently carries no data and exists so that the operator has a concrete context
 * instance to pass to the plugins. Fields can be added later without breaking the plugins.
 */
public class GatekeeperKafkaUserEntryContextImpl implements GatekeeperKafkaUserEntryContext {
    /**
     * Creates the KafkaUser entry context.
     */
    public GatekeeperKafkaUserEntryContextImpl() { }
}
