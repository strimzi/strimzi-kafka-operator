/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.common.Reconciliation;

import java.util.ArrayList;
import java.util.List;

/**
 * AbstractStatefulModel an abstract model class for components which use Storage. It is currently used by Kafka and
 * ZooKeeper clusters.
 */
public abstract class AbstractStatefulModel extends AbstractModel {
    /**
     * Storage configuration
     */
    protected Storage storage;

    /**
     * Warning conditions generated from the Custom Resource
     */
    protected List<Condition> warningConditions = new ArrayList<>(0);

    /**
     * Constructor
     *
     * @param reconciliation    The reconciliation marker
     * @param resource          Custom resource with metadata containing the namespace and cluster name
     * @param componentName     Name of the Strimzi component usually consisting from the cluster name and component type
     * @param componentType     Type of the component that the extending class is deploying (e.g. Kafka, ZooKeeper etc. )
     */
    protected AbstractStatefulModel(Reconciliation reconciliation, HasMetadata resource, String componentName, String componentType) {
        super(reconciliation, resource, componentName, componentType);
    }

    /**
     * @return The storage.
     */
    public Storage getStorage() {
        return storage;
    }

    /**
     * Set the Storage
     *
     * @param storage Persistent Storage configuration
     */
    protected void setStorage(Storage storage) {
        StorageUtils.validatePersistentStorage(storage);
        this.storage = storage;
    }

    /**
     * Returns a list of warning conditions set by the model. Returns an empty list if no warning conditions were set.
     *
     * @return  List of warning conditions.
     */
    public List<Condition> getWarningConditions() {
        return warningConditions;
    }
}
