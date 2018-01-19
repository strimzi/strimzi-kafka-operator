package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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

            // match-expressions doesn't supported yet
            List<LabelSelectorRequirement> matchExpressions = null;

            storage.withSelector(new LabelSelector(matchExpressions, matchLabels));
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
     * Compute the difference between two Storage instances
     *
     * @param other the other instance to compare with
     * @return  the result with all differences
     */
    public StorageDiffResult diff(Storage other) {

        StorageDiffResult diffResult = new StorageDiffResult();

        diffResult
                .withDifferentType(this.type != other.type())
                .withDifferentSize(!this.compareSize(other.size()))
                .withDifferentDeleteClaim(this.isDeleteClaim != other.isDeleteClaim())
                .withDifferentStorageClass(!this.compareStorageClass(other.storageClass()))
                .withDifferentSelector(!this.compareSelector(other.selector()));


        return diffResult;
    }

    /**
     * Compare two Storage sizes
     *
     * @param other the other Storage size
     * @return  if the compared Storage sizes are equals
     */
    private boolean compareSize(Quantity other) {

        return Objects.isNull(this.size) ?
                Objects.isNull(other) : this.size.getAmount().equals(other.getAmount());
    }

    /**
     * Compare two Storage classes
     *
     * @param other the other Storage class
     * @return  if the compared Storage classes are equals
     */
    private boolean compareStorageClass(String other) {

        return Objects.isNull(this.storageClass) ?
                Objects.isNull(other) : this.storageClass.equals(other);
    }

    /**
     * Compare two selectors
     *
     * @param other the other selector
     * @return  if the compared selectors are equals
     */
    private boolean compareSelector(LabelSelector other) {

        if (!Objects.isNull(this.selector) && !Objects.isNull(other)) {

            if (Objects.isNull(this.selector.getMatchLabels()) && !Objects.isNull(other.getMatchLabels())) {
                return false;
            }

            if (!Objects.isNull(this.selector.getMatchLabels()) && Objects.isNull(other.getMatchLabels())) {
                return false;
            }

            if (this.selector.getMatchLabels().size() != other.getMatchLabels().size()) {
                return false;
            }

            return this.selector.getMatchLabels().entrySet().equals(other.getMatchLabels().entrySet());

        } else if (Objects.isNull(this.selector) && Objects.isNull(other)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Result after comparing two Storage instances
     */
    public class StorageDiffResult {

        private boolean isType;
        private boolean isSize;
        private boolean isStorageClass;
        private boolean isSelector;
        private boolean isDeleteClaim;

        /**
         * @return  if the Storage type is different
         */
        public boolean isType() {
            return this.isType;
        }

        /**
         * @return  if the Storage size is different
         */
        public boolean isSize() {
            return this.isSize;
        }

        /**
         * @return  if the Storage class is different
         */
        public boolean isStorageClass() {
            return this.isStorageClass;
        }

        /**
         * @return  if the selector is different
         */
        public boolean isSelector() {
            return this.isSelector;
        }

        /**
         * @return  if the deleteClaim is different
         */
        public boolean isDeleteClaim() {
            return this.isDeleteClaim;
        }

        /**
         * Set if the Storage type is different
         *
         * @param isType    if type is different
         * @return  current StorageDiffResult instance
         */
        public StorageDiffResult withDifferentType(boolean isType) {
            this.isType = isType;
            return this;
        }

        /**
         * Set if the Storage size is different
         *
         * @param isSize    if size is different
         * @return  current StorageDiffResult instance
         */
        public StorageDiffResult withDifferentSize(boolean isSize) {
            this.isSize = isSize;
            return this;
        }

        /**
         * Set if the Storage class is different
         *
         * @param isStorageClass    if storage class is different
         * @return  current StorageDiffResult instance
         */
        public StorageDiffResult withDifferentStorageClass(boolean isStorageClass) {
            this.isStorageClass = isStorageClass;
            return this;
        }

        /**
         * Set if the selector is different
         *
         * @param isSelector    if the selector is different
         * @return  current StorageDiffResult instance
         */
        public StorageDiffResult withDifferentSelector(boolean isSelector) {
            this.isSelector = isSelector;
            return this;
        }

        /**
         * Set if the deleteClaim is different
         *
         * @param isDeleteClaim if the deleteClaim is different
         * @return  current StorageDiffResult instance
         */
        public StorageDiffResult withDifferentDeleteClaim(boolean isDeleteClaim) {
            this.isDeleteClaim = isDeleteClaim;
            return this;
        }
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
