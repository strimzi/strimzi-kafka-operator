/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverride;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.common.operator.resource.AbstractResourceDiff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

/**
 * Class for diffing storage configuration
 */
public class StorageDiff extends AbstractResourceDiff {
    private static final Logger log = LogManager.getLogger(StorageDiff.class.getName());

    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/deleteClaim|/)$");

    private final boolean isEmpty;
    private final boolean changesType;
    private final boolean shrinkSize;

    /**
     * Diffs the storage for allowed or not allowed changes. Examples of allowed changes is increasing volume size or
     * adding overrides for nodes before scale-up / removing them after scale-down.
     *
     * @param current           Current Storage configuration
     * @param desired           Desired Storage configuration
     * @param currentReplicas   Current number of replicas (will differ from desired number of replicas when scaling up or down)
     * @param desiredReplicas   Desired number of replicas (will differ from current number of replicas when scaling up or down)
     */
    public StorageDiff(Storage current, Storage desired, int currentReplicas, int desiredReplicas) {
        this(current, desired, currentReplicas, desiredReplicas, "");
    }

    /**
     * Diffs the storage for allowed or not allowed changes. Examples of allowed changes is increasing volume size or
     * adding overrides for nodes before scale-up / removing them after scale-down. This constructor is used internally
     * only.
     *
     * @param current           Current Storage configuration
     * @param desired           Desired Storage configuration
     * @param currentReplicas   Current number of replicas (will differ from desired number of replicas when scaling up or down)
     * @param desiredReplicas   Desired number of replicas (will differ from current number of replicas when scaling up or down)
     * @param volumeDesc        Description of the volume which is being used
     */
    private StorageDiff(Storage current, Storage desired, int currentReplicas, int desiredReplicas, String volumeDesc) {
        boolean changesType = false;
        boolean shrinkSize = false;
        boolean isEmpty = true;

        if (current instanceof JbodStorage && desired instanceof JbodStorage) {
            Set<Integer> volumeIds = new HashSet<>();

            volumeIds.addAll(((JbodStorage) current).getVolumes().stream().map(SingleVolumeStorage::getId).collect(Collectors.toSet()));
            volumeIds.addAll(((JbodStorage) desired).getVolumes().stream().map(SingleVolumeStorage::getId).collect(Collectors.toSet()));

            for (Integer volumeId : volumeIds)  {
                SingleVolumeStorage currentVolume = ((JbodStorage) current).getVolumes().stream()
                        .filter(volume -> volume != null && volumeId.equals(volume.getId()))
                        .findAny().orElse(null);
                SingleVolumeStorage desiredVolume = ((JbodStorage) desired).getVolumes().stream()
                        .filter(volume -> volume != null && volumeId.equals(volume.getId()))
                        .findAny().orElse(null);

                StorageDiff diff = new StorageDiff(currentVolume, desiredVolume, currentReplicas, desiredReplicas, "(volume ID: " + volumeId + ") ");

                changesType |= diff.changesType();
                shrinkSize |= diff.shrinkSize();
                isEmpty &= diff.isEmpty();
            }
        } else {
            JsonNode source = patchMapper().valueToTree(current == null ? "{}" : current);
            JsonNode target = patchMapper().valueToTree(desired == null ? "{}" : desired);
            JsonNode diff = JsonDiff.asJson(source, target);

            int num = 0;

            for (JsonNode d : diff) {
                String pathValue = d.get("path").asText();

                if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                    log.debug("Ignoring Storage {}diff {}", volumeDesc, d);
                    continue;
                }

                // It might be possible to increase the volume size, but never to shrink volumes
                // When size changes, we need to detect whether it is shrinking or increasing
                if (pathValue.endsWith("/size") && desired.getType().equals(current.getType()) && current instanceof PersistentClaimStorage && desired instanceof PersistentClaimStorage)    {
                    PersistentClaimStorage persistentCurrent = (PersistentClaimStorage) current;
                    PersistentClaimStorage persistentDesired = (PersistentClaimStorage) desired;

                    long currentSize = StorageUtils.parseMemory(persistentCurrent.getSize());
                    long desiredSize = StorageUtils.parseMemory(persistentDesired.getSize());

                    if (currentSize > desiredSize) {
                        shrinkSize = true;
                    } else {
                        continue;
                    }
                }

                // Some changes to overrides are allowed:
                // * When scaling up or down, you can set the overrides for new nodes
                // * You can set overrides for nodes which do nto exist (yet)
                if (pathValue.startsWith("/overrides")) {
                    if (isOverrideChangeAllowed(current, desired, currentReplicas, desiredReplicas))    {
                        continue;
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("Storage {}differs: {}", volumeDesc, d);
                    log.debug("Current Storage {}path {} has value {}", volumeDesc, pathValue, lookupPath(source, pathValue));
                    log.debug("Desired Storage {}path {} has value {}", volumeDesc, pathValue, lookupPath(target, pathValue));
                }

                num++;
                changesType |= pathValue.endsWith("/type");
            }

            isEmpty = num == 0;
        }

        this.isEmpty = isEmpty;
        this.changesType = changesType;
        this.shrinkSize = shrinkSize;
    }

    /**
     * Returns whether the Diff is empty or not
     *
     * @return true when the storage configurations are the same
     */
    @Override
    public boolean isEmpty() {
        return isEmpty;
    }

    /**
     * Returns true if there's a difference in {@code /type}
     *
     * @return true when the storage configurations have different type
     */
    public boolean changesType() {
        return changesType;
    }

    /**
     * Returns true if there's a difference in {@code /size}
     *
     * @return true when the size of the volumes changed
     */
    public boolean shrinkSize() {
        return shrinkSize;
    }

    /**
     * Validates the changes to the storage overrides and decides whether they are allowed or not. Allowed changes are
     * those to nodes which will be added, removed or which do nto exist yet.
     *
     * @param current           Current Storage configuration
     * @param desired           New storage configuration
     * @param currentReplicas   Current number of replicas
     * @param desiredReplicas   Desired number of replicas
     * @return                  True if only allowed override changes were done, false othewise
     */
    private boolean isOverrideChangeAllowed(Storage current, Storage desired,  int currentReplicas, int desiredReplicas)   {
        List<PersistentClaimStorageOverride> currentOverrides = ((PersistentClaimStorage) current).getOverrides();
        if (currentOverrides == null)   {
            currentOverrides = Collections.emptyList();
        }

        List<PersistentClaimStorageOverride> desiredOverrides = ((PersistentClaimStorage) desired).getOverrides();
        if (desiredOverrides == null)   {
            desiredOverrides = Collections.emptyList();
        }

        // We care only about the nodes which existed before this reconciliation and will still exist after it
        int existedAndWillExist = Math.min(currentReplicas, desiredReplicas);

        for (int i = 0; i < existedAndWillExist; i++)    {
            int nodeId = i;

            PersistentClaimStorageOverride currentOverride = currentOverrides.stream()
                    .filter(override -> override.getBroker() == nodeId)
                    .findFirst()
                    .orElse(null);

            PersistentClaimStorageOverride desiredOverride = desiredOverrides.stream()
                    .filter(override -> override.getBroker() == nodeId)
                    .findFirst()
                    .orElse(null);

            if (currentOverride != null && desiredOverride != null) {
                // Both overrides exist but are not equal
                if (!currentOverride.equals(desiredOverride)) {
                    return false;
                }
            } else if (currentOverride != null || desiredOverride != null) {
                // One of them is null while the other is not null => they differ
                return false;
            }
        }

        return true;
    }
}
