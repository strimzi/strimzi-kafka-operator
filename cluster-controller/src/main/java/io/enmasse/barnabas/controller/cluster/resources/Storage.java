package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents information about storage to use
 */
public class Storage {

    private static final String TYPE_FIELD = "type";
    private static final String SIZE_FIELD = "size";
    private static final String STORAGE_CLASS_FIELD = "class";
    private static final String SELECTOR_FIELD = "selector";

    private final StorageType type;
    private Quantity size;
    private String storageClass;
    private Map<String, String> selector;

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
    public Storage withSelector(final Map<String, String> selector) {
        this.selector = selector;
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

        // TODO : getting the storage selector

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
            storage.withSelector(new HashMap<>(pvc.getSpec().getSelector().getMatchLabels()));
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
                throw new IllegalArgumentException("Unknown type: " + type);
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
    public Map<String, String> selector() {
        return this.selector;
    }
}
