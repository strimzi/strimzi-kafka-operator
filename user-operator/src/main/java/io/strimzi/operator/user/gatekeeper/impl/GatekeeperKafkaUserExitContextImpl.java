/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.gatekeeper.impl;

import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserExitContext;

/**
 * Default implementation of the {@link GatekeeperKafkaUserExitContext} passed to the KafkaUser plugins after a KafkaUser
 * reconciliation completes. It currently carries no data and exists so that the operator has a concrete context instance
 * to pass to the plugins. Fields can be added later without breaking the plugins.
 */
public class GatekeeperKafkaUserExitContextImpl implements GatekeeperKafkaUserExitContext {
    /**
     * Creates the KafkaUser exit context.
     */
    public GatekeeperKafkaUserExitContextImpl() { }
}
