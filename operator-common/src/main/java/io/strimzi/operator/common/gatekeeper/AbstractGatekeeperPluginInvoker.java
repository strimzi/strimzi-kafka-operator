/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper;

import io.strimzi.plugin.gatekeeper.GatekeeperPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Abstract class for invoking the Strimzi Gatekeeper plugins during reconciliations. It provides a common
 * implementation of the plugin invocation logic, which is used by the concrete invokers for each operator.
 */
public abstract class AbstractGatekeeperPluginInvoker {
    /**
     * The reconciliation phase in which the plugins are invoked. It determines the order in which they are called:
     * the configured order for the entry phase and the reverse order for the exit phase.
     */
    protected enum Phase {
        ENTRY,
        EXIT
    }

    /**
     * Invokes a single chain of plugins for one operand and phase. It loads the plugins for the given operand from the
     * factory, orders them according to the phase (the configured order for the entry phase, the reverse for the exit
     * phase) and iterates them. Based on the interface each plugin implements, it is invoked either as a mutating or as a
     * validating plugin. A mutating plugin receives the current value, and the (possibly modified) value it returns is
     * passed to the next plugin. A validating plugin is invoked through the {@code validate} function (which is expected
     * to pass it only copies of the resources and status) and the original value is passed on unchanged. The chain stops,
     * completing exceptionally, as soon as any plugin fails.
     *
     * @param mutatingType      The mutating plugin interface for this operand
     * @param validatingType    The validating plugin interface for this operand
     * @param phase             The reconciliation phase, determining the order in which the plugins are invoked
     * @param initialValue      The initial value passed to the first plugin
     * @param mutate            Function invoking a mutating plugin with the current value
     * @param validate          Function invoking a validating plugin with the current value
     * @param <M>               The type of the mutating plugin
     * @param <D>               The type of the validating plugin
     * @param <V>               The type of the value chained through the plugins
     *
     * @return  A completion stage with the value produced by the last plugin (or the initial value if there are none)
     */
    protected static <M, D, V> CompletionStage<V> chain(
            Class<M> mutatingType,
            Class<D> validatingType,
            Phase phase,
            V initialValue,
            BiFunction<M, V, CompletionStage<V>> mutate,
            BiFunction<D, V, CompletionStage<Void>> validate) {
        // Collect the plugins of this operand - both mutating and validating - preserving the configured order
        List<GatekeeperPlugin> plugins = new ArrayList<>();
        for (GatekeeperPlugin plugin : GatekeeperPluginFactory.getPlugins()) {
            if (mutatingType.isInstance(plugin) || validatingType.isInstance(plugin)) {
                plugins.add(plugin);
            }
        }

        if (phase == Phase.EXIT) {
            Collections.reverse(plugins);
        }

        CompletionStage<V> result = CompletableFuture.completedFuture(initialValue);

        for (GatekeeperPlugin plugin : plugins) {
            if (mutatingType.isInstance(plugin)) {
                M mutating = mutatingType.cast(plugin);
                result = result.thenCompose(current -> mutate.apply(mutating, current));
            } else if (validatingType.isInstance(plugin)) {
                D validating = validatingType.cast(plugin);
                result = result.thenCompose(current -> validate.apply(validating, current).thenApply(ignored -> current));
            }
        }

        return result;
    }

    /**
     * Creates a deep copy of a value for a validating plugin, but only when the value is not {@code null}. A
     * {@code null} value (for example, a custom resource or status during a deletion) is passed through as {@code null}
     * instead of being turned into an empty copy, so that the plugins can recognize it.
     *
     * @param value     The value to copy, or {@code null}
     * @param copy      Function creating the deep copy
     * @param <T>       The type of the value
     *
     * @return  A deep copy of the value, or {@code null} when the value is {@code null}
     */
    protected static <T> T copy(T value, UnaryOperator<T> copy) {
        return value == null ? null : copy.apply(value);
    }

    /**
     * Creates a deep copy of a list of values for a validating plugin, but only when the list is not {@code null}. Each
     * item is copied using the given function. A {@code null} list is passed through as {@code null}.
     *
     * @param values    The list of values to copy, or {@code null}
     * @param copy      Function creating the deep copy of a single item
     * @param <T>       The type of the items
     *
     * @return  A new list with the deep copies of the items, or {@code null} when the list is {@code null}
     */
    protected static <T> List<T> copyList(List<T> values, UnaryOperator<T> copy) {
        return values == null ? null : values.stream().map(copy).toList();
    }

    /**
     * Invokes the deletion hooks of all the plugins of one operand - both mutating and validating - as a single ordered
     * chain. The plugins are collected in the configured order, and the chain stops, completing exceptionally, as soon as
     * one of them fails. The deletion hook is declared separately on the mutating and validating plugin interfaces, so
     * the caller's {@code invoke} function performs the dispatch to whichever of the two the plugin implements.
     *
     * @param mutatingType      The mutating plugin interface of the operand
     * @param validatingType    The validating plugin interface of the operand
     * @param invoke            Function invoking the deletion hook of one plugin (a mutating or validating plugin)
     *
     * @return  A completion stage that completes when all the deletion hooks completed
     */
    protected static CompletionStage<Void> deletion(Class<? extends GatekeeperPlugin> mutatingType, Class<? extends GatekeeperPlugin> validatingType, Function<GatekeeperPlugin, CompletionStage<Void>> invoke) {
        CompletionStage<Void> result = CompletableFuture.completedFuture(null);

        for (GatekeeperPlugin plugin : GatekeeperPluginFactory.getPlugins()) {
            if (mutatingType.isInstance(plugin) || validatingType.isInstance(plugin)) {
                result = result.thenCompose(ignored -> invoke.apply(plugin));
            }
        }

        return result;
    }
}
