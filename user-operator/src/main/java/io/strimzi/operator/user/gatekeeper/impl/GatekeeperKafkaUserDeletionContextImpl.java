/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.gatekeeper.impl;

import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserDeletionContext;

/**
 * Default implementation of the {@link GatekeeperKafkaUserDeletionContext} passed to the KafkaUser plugins when a KafkaUser
 * is being deleted. It currently carries no data and exists so that the operator has a concrete context instance to pass
 * to the plugins. Fields can be added later without breaking the plugins.
 */
public class GatekeeperKafkaUserDeletionContextImpl implements GatekeeperKafkaUserDeletionContext {
    /**
     * Creates the KafkaUser deletion context.
     */
    public GatekeeperKafkaUserDeletionContextImpl() { }
}
