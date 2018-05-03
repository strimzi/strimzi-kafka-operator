/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents information about storage to use
 */
public class Storage {

    public static final String TYPE_FIELD = "type";
    public static final String SIZE_FIELD = "size";
    public static final String STORAGE_CLASS_FIELD = "class";
    public static final String SELECTOR_FIELD = "selector";
    public static final String SELECTOR_MATCH_LABELS_FIELD = "match-labels";
    public static final String DELETE_CLAIM_FIELD = "delete-claim";

    private final StorageType type;
    private Quantity size;
    private String storageClass;
    private LabelSelector selector;
    private boolean isDeleteClaim = false;

    /**
     * Constructor
     *
     * @param type  storage type (it's mandatory)
     */
    public Storage(StorageType type) {
        this.type = type;
    }

    /**
     * Specify the storage size
     *
     * @param size  storage size
     * @return  current Storage instance
     */
    public Storage withSize(final Quantity size) {
        this.size = size;
        return this;
    }

    /**
     * Specify the storage class to use for dynamic provisioning
     *
     * @param storageClass  storage class name
     * @return  current Storage instance
     */
    public Storage withClass(final String storageClass) {
        this.storageClass = storageClass;
        return this;
    }

    /**
     * Specify a selector for selecting specific storage
     *
     * @param selector  map with labels=values for selecting storage
     * @return  current Storage instance
     */
    public Storage withSelector(final LabelSelector selector) {
        this.selector = selector;
        return this;
    }

    /**
     * Specify if the claims (for "persistent-claim" type) have to be deleted
     * when the entire cluster is deleted
     *
     * @param isDeleteClaim if claims have to be deleted
     * @return  current Storage instance
     */
    public Storage withDeleteClaim(final boolean isDeleteClaim) {
        this.isDeleteClaim = isDeleteClaim;
        return this;
    }

    /**
     * Returns a Storage instance from a corresponding JSON representation
     *
     * @param json  storage JSON representation
     * @return  Storage instance
     */
    public static Storage fromJson(JsonObject json) {

        String type = json.getString(Storage.TYPE_FIELD);
        if (type == null) {
            throw new IllegalArgumentException("Storage '" + Storage.TYPE_FIELD + "' is mandatory");
        }

        Storage storage = new Storage(StorageType.from(type));

        String size = json.getString(Storage.SIZE_FIELD);
        if (size != null) {
            storage.withSize(new Quantity(size));
        }
        String storageClass = json.getString(Storage.STORAGE_CLASS_FIELD);
        if (storageClass != null) {
            storage.withClass(storageClass);
        }

        if (json.getValue(Storage.DELETE_CLAIM_FIELD) instanceof Boolean) {
            boolean isDeleteClaim = json.getBoolean(Storage.DELETE_CLAIM_FIELD);
            storage.withDeleteClaim(isDeleteClaim);
        }

        JsonObject selector = json.getJsonObject(Storage.SELECTOR_FIELD);
        if (selector != null) {

            JsonObject matchLabelsJson = selector.getJsonObject(Storage.SELECTOR_MATCH_LABELS_FIELD);
            Map<String, String> matchLabels =
                    matchLabelsJson.getMap().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));

            // match-expressions doesn't supported yet, so null first argument
            storage.withSelector(new LabelSelector(null, matchLabels));
        }

        return storage;
    }

    /**
     * Returns a Storage instance from a corresponding PersistentVolumeClaim
     *
     * @param pvc   PersistentVolumeClaim representation
     * @return  Storage instance
     */
    public static Storage fromPersistentVolumeClaim(PersistentVolumeClaim pvc) {

        Storage storage = new Storage(StorageType.PERSISTENT_CLAIM);
        storage.withSize(pvc.getSpec().getResources().getRequests().get("storage"))
                .withClass(pvc.getSpec().getStorageClassName());

        if (pvc.getSpec().getSelector() != null) {
            storage.withSelector(pvc.getSpec().getSelector());
        }

        return storage;
    }


    /**
     * Storage type Kubernetes/OpenShift oriented
     */
    public enum StorageType {

        EPHEMERAL("ephemeral"),
        PERSISTENT_CLAIM("persistent-claim"),
        LOCAL("local");

        private final String type;

        private StorageType(String type) {
            this.type = type;
        }

        /**
         * Get the storage type from a string representation
         *
         * @param type  string representation for the storage type
         * @return  storage type
         */
        public static StorageType from(String type) {
            if (type.equals(EPHEMERAL.type)) {
                return EPHEMERAL;
            } else if (type.equals(PERSISTENT_CLAIM.type)) {
                return PERSISTENT_CLAIM;
            } else if (type.equals(LOCAL.type)) {
                return LOCAL;
            } else {
                throw new IllegalArgumentException("Unknown type: " + type + ". Allowed types are: " + EPHEMERAL.toString() + ", " + PERSISTENT_CLAIM.toString() + " and " + LOCAL.toString() + ".");
            }
        }
    }

    /**
     * @return  storage type
     */
    public StorageType type() {
        return this.type;
    }

    /**
     * @return  storage size
     */
    public Quantity size() {
        return this.size;
    }

    /**
     * @return  storage class name
     */
    public String storageClass() {
        return this.storageClass;
    }

    /**
     * @return  map with labels=values for selecting storage
     */
    public LabelSelector selector() {
        return this.selector;
    }

    /**
     * @return  if the claims (for "persistent-claim" type) have to be deleted
     *          when the entire cluster is deleted
     */
    public boolean isDeleteClaim() {
        return this.isDeleteClaim;
    }
}
