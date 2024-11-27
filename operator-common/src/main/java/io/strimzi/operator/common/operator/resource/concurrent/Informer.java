/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A class that wraps the Fabric8 Informer and Lister for particular resource
 *
 * @param <T>   Type of the resource handled by this informer
 */
public class Informer<T extends HasMetadata> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Informer.class);

    private final SharedIndexInformer<T> informer;
    private final Lister<T> lister;

    /**
     * Constructor.
     *
     * @param informer  The Fabric8 informer that will be wrapped inside this class
     */
    public Informer(SharedIndexInformer<T> informer) {
        this.informer = informer;
        this.lister = new Lister<>(informer.getIndexer());

        // Setup the exception handler to log errors
        informer.exceptionHandler((isStarted, t) -> {
            LOGGER.errorOp("Caught exception in the {} informer which is {}", informer.getApiTypeClass().getSimpleName(), (isStarted ? "started" : "not started"), t);
            // We always want the informer to retry => we just want to log the error
            return true;
        });
    }

    //////////////////////////////
    /// Methods for working with the lister cache and querying the informer
    //////////////////////////////

    /**
     * Finds the resource based on its namespace and name
     *
     * @param namespace     Namespace of the resource
     * @param name          Name of the resource
     *
     * @return  The resource matching the name and namespace or null if it was not found
     */
    public T get(String namespace, String name)   {
        return lister.namespace(namespace).get(name);
    }

    /**
     * Lists the resource from a namespace
     *
     * @param namespace     Namespace of the resource
     *
     * @return  List of resources that exist in a given namespace
     */
    public List<T> list(String namespace)   {
        return lister.namespace(namespace).list();
    }


    //////////////////////////////
    /// "Inherited" methods for working with the informers -> just call the corresponding informer method
    //////////////////////////////

    /**
     * Configures the event handler for this informer
     *
     * @param handler   Event handler
     */
    public void addEventHandler(ResourceEventHandler<T> handler)   {
        informer.addEventHandler(handler);
    }

    /**
     * Starts the informer
     *
     * @return  CompletionStage that completes when the informer is started
     */
    public CompletionStage<Void> start() {
        CompletionStage<Void> start = informer.start();

        // Setup logging for when the informer stops -> this is useful to debug various failures with the informers
        informer.stopped().whenComplete((v, t) -> {
            if (t != null) {
                LOGGER.warnOp("{} informer stopped", informer.getApiTypeClass().getSimpleName(), t);
            } else {
                LOGGER.infoOp("{} informer stopped", informer.getApiTypeClass().getSimpleName());
            }
        });

        return start;
    }

    /**
     * Stops the informer
     */
    public void stop() {
        informer.stop();
    }

    /**
     * Waits until the informer is stopped
     *
     * @return  CompletionStage that completes when the informer is stopped
     */
    public CompletionStage<Void> stopped() {
        return informer.stopped();
    }

    /**
     * Indicates whether the informer is synced
     *
     * @return  True if the informer is synced. False otherwise.
     */
    public boolean hasSynced()    {
        return informer.hasSynced();
    }

    /**
     * Indicates whether the informer is running
     *
     * @return  True if the informer is running. False otherwise.
     */
    public boolean isRunning()    {
        return informer.isRunning();
    }
}