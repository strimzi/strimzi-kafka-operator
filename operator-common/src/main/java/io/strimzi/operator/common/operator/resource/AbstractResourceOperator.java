/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractResourceOperator<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {
    /**
     * Default reconciliation timeout
     */
    private static final long DEFAULT_TIMEOUT_MS = 300_000;

    protected final Vertx vertx;
    protected final C client;
    protected final String resourceKind;
    protected final ResourceSupport resourceSupport;

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractResourceOperator(Vertx vertx, C client, String resourceKind) {
        this.vertx = vertx;
        this.resourceSupport = new ResourceSupport(vertx);
        this.client = client;
        this.resourceKind = resourceKind;
    }

    /**
     * @return  Default timeout for deleting resources
     */
    protected long deleteTimeoutMs() {
        return DEFAULT_TIMEOUT_MS;
    }

    /**
     * @return  Returns the Pattern for matching paths which can be ignored in the resource diff
     */
    protected Pattern ignorablePaths() {
        return ResourceDiff.DEFAULT_IGNORABLE_PATHS;
    }

    /**
     * Returns the diff of the current and desired resources
     *
     * @param reconciliation The reconciliation
     * @param resourceName  Name of the resource used for logging
     * @param current       Current resource
     * @param desired       Desired resource
     *
     * @return              The ResourceDiff instance
     */
    protected ResourceDiff<T> diff(Reconciliation reconciliation, String resourceName, T current, T desired)  {
        return new ResourceDiff<>(reconciliation, resourceKind, resourceName, current, desired, ignorablePaths());
    }

    /**
     * Checks whether the current and desired resources differ and need to be patched in the Kubernetes API server.
     *
     * @param reconciliation The reconciliation
     * @param name      Name of the resource used for logging
     * @param current   Current resource
     * @param desired   Desired resource
     *
     * @return          True if the resources differ and need patching
     */
    protected boolean needsPatching(Reconciliation reconciliation, String name, T current, T desired)   {
        return !diff(reconciliation, name, current, desired).isEmpty();
    }

    /**
     * Compares two resources and decides whether they changed or not based on their resource versions form metadata.
     *
     * @param oldVersion    Old resource
     * @param newVersion    New resource
     *
     * @return  True if the resource changed. False otherwise.
     */
    protected boolean wasChanged(T oldVersion, T newVersion) {
        if (oldVersion != null
                && oldVersion.getMetadata() != null
                && newVersion != null
                && newVersion.getMetadata() != null) {
            return !Objects.equals(oldVersion.getMetadata().getResourceVersion(), newVersion.getMetadata().getResourceVersion());
        } else {
            return true;
        }
    }

    /**
     * Applies the selector to the operation. If the selector is specified, it will return an operation with the applied
     * selector. If it is not specified, it will return the original operation.
     *
     * @param filterable    Filterable operation on which we can apply the selector
     * @param selector      Selector
     *
     * @return  Filtered operation
     */
    protected FilterWatchListDeletable<T, L, R> applySelector(FilterWatchListDeletable<T, L, R> filterable, Labels selector)  {
        if (selector != null) {
            return filterable.withLabels(selector.toMap());
        } else {
            return filterable;
        }
    }

    /**
     * Applies the selector to the operation. If the selector is specified, it will return an operation with the applied
     * selector. If it is not specified, it will return the original operation.
     *
     * @param filterable    Filterable operation on which we can apply the selector
     * @param selector      Selector
     *
     * @return  Filtered operation
     */
    protected FilterWatchListDeletable<T, L, R> applySelector(FilterWatchListDeletable<T, L, R> filterable, Optional<LabelSelector> selector)  {
        if (selector.isPresent()) {
            return filterable.withLabelSelector(selector.get());
        } else {
            return filterable;
        }
    }

    /**
     * List the resources and returns Java list with all found resources.
     *
     * @param listable  Listable operation
     *
     * @return  List of resources
     */
    protected List<T> list(Listable<L> listable)    {
        return listable.list(new ListOptionsBuilder().withResourceVersion("0").build()).getItems();
    }

    /**
     * List the resources and returns Java list with all found resources in asynchronous way.
     *
     * @param listable  Listable operation
     *
     * @return  Future with the list of resources
     */
    protected Future<List<T>> listAsync(Listable<L> listable) {
        return resourceSupport.listAsync(listable);
    }
}
