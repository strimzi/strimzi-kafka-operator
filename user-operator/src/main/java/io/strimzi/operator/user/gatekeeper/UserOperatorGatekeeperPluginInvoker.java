/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.gatekeeper;

import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.api.kafka.model.user.KafkaUserStatusBuilder;
import io.strimzi.operator.common.gatekeeper.AbstractGatekeeperPluginInvoker;
import io.strimzi.operator.common.gatekeeper.GatekeeperPluginFactory;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserValidatingPlugin;

import java.util.concurrent.CompletionStage;

/**
 * Invokes the Strimzi Gatekeeper plugins for KafkaUser reconciliations. It provides entry, exit, and deletion methods
 * which invoke all the KafkaUser plugins - both mutating and validating - as one ordered chain. The plugins (and their
 * order) are provided by the {@link GatekeeperPluginFactory}.
 */
public class UserOperatorGatekeeperPluginInvoker extends AbstractGatekeeperPluginInvoker {
    private UserOperatorGatekeeperPluginInvoker() { }

    /**
     * Invokes the entry of all the KafkaUser plugins (mutating and validating) as a single-ordered chain. Mutating plugins
     * can modify the resource; validating plugins receive a copy of it and let the original pass through unchanged.
     *
     * @param context   The entry context passed to the plugins
     * @param kafkaUser   The KafkaUser custom resource being reconciled
     *
     * @return  A completion stage with the KafkaUser resource after it passed through all the plugins
     */
    public static CompletionStage<KafkaUser> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
        return chain(
                GatekeeperKafkaUserMutatingPlugin.class,
                GatekeeperKafkaUserValidatingPlugin.class,
                Phase.ENTRY,
                kafkaUser,
                (plugin, current) -> plugin.kafkaUserEntry(context, current),
                (plugin, current) -> plugin.kafkaUserEntry(context, copy(current, item -> new KafkaUserBuilder(item).build())));
    }

    /**
     * Invokes the exit of all the KafkaUser plugins (mutating and validating) as a single-ordered chain, in the reverse of
     * the configured order. Mutating plugins can modify the status; validating plugins receive copies of the resource and
     * the status and let the original status pass through unchanged.
     *
     * @param context   The exit context passed to the plugins
     * @param kafkaUser   The KafkaUser custom resource being reconciled
     * @param status    The status computed for the KafkaUser resource
     *
     * @return  A completion stage with the KafkaUser status after it passed through all the plugins
     */
    public static CompletionStage<KafkaUserStatus> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus status) {
        return chain(
                GatekeeperKafkaUserMutatingPlugin.class,
                GatekeeperKafkaUserValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, currentStatus) -> plugin.kafkaUserExit(context, kafkaUser, currentStatus),
                (plugin, currentStatus) -> plugin.kafkaUserExit(context, copy(kafkaUser, item -> new KafkaUserBuilder(item).build()), copy(currentStatus, item -> new KafkaUserStatusBuilder(item).build())));
    }

    /**
     * Invokes the deletion hook of all the KafkaUser plugins as a single ordered chain. There is no resource to mutate
     * during a deletion, so the hooks only react to the deletion; any of them can reject it by completing exceptionally,
     * which stops the chain.
     *
     * @param context   The deletion context passed to the plugins
     * @param namespace The namespace of the KafkaUser being deleted
     * @param name      The name of the KafkaUser being deleted
     *
     * @return  A completion stage that completes when all the deletion hooks completed
     */
    public static CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
        return deletion(
                GatekeeperKafkaUserMutatingPlugin.class,
                GatekeeperKafkaUserValidatingPlugin.class,
                plugin -> plugin instanceof GatekeeperKafkaUserMutatingPlugin mutating
                        ? mutating.kafkaUserDeletion(context, namespace, name)
                        : ((GatekeeperKafkaUserValidatingPlugin) plugin).kafkaUserDeletion(context, namespace, name));
    }
}
