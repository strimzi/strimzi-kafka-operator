/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;

/**
 * Shared methods for working with Volume
 */
public class VolumeUtils {

    protected static final Logger log = LogManager.getLogger(VolumeUtils.class.getName());

    /**
     * Creates a secret volume with given items
     *
     * @param name        Name of the Volume
     * @param secretName  Name of the Secret
     * @param items       contents of the Secret
     * @param isOpenshift true if underlying cluster OpenShift
     */
    public static Volume createSecretVolume(String name, String secretName, Map<String, String> items, boolean isOpenshift) {
        int mode = 0444;
        if (isOpenshift) {
            mode = 0440;
        }

        List<KeyToPath> keysPaths = new ArrayList<>();

        for (Map.Entry<String, String> item : items.entrySet()) {
            KeyToPath keyPath = new KeyToPathBuilder()
                    .withNewKey(item.getKey())
                    .withNewPath(item.getValue())
                    .build();

            keysPaths.add(keyPath);
        }

        SecretVolumeSource secretVolumeSource = new SecretVolumeSourceBuilder()
                .withDefaultMode(mode)
                .withSecretName(secretName)
                .withItems(keysPaths)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withSecret(secretVolumeSource)
                .build();
        log.trace("Created secret Volume named '{}' with source secret '{}'", name, secretName);
        return volume;
    }

    /**
     * Creates a secret volume
     *
     * @param name        Name of the Volume
     * @param secretName  Name of the Secret
     * @param isOpenshift true if underlying cluster OpenShift
     */
    public static Volume createSecretVolume(String name, String secretName, boolean isOpenshift) {
        int mode = 0444;
        if (isOpenshift) {
            mode = 0440;
        }

        SecretVolumeSource secretVolumeSource = new SecretVolumeSourceBuilder()
                .withDefaultMode(mode)
                .withSecretName(secretName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withSecret(secretVolumeSource)
                .build();
        log.trace("Created secret Volume named '{}' with source secret '{}'", name, secretName);
        return volume;
    }

    /**
     * Creates an empty directory volume
     *
     * @param name      Name of the Volume
     * @param sizeLimit Volume size
     */
    public static Volume createEmptyDirVolume(String name, String sizeLimit) {
        EmptyDirVolumeSource emptyDirVolumeSource = new EmptyDirVolumeSourceBuilder().build();
        if (sizeLimit != null && !sizeLimit.isEmpty()) {
            emptyDirVolumeSource.setSizeLimit(new Quantity(sizeLimit));
        }

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withEmptyDir(emptyDirVolumeSource)
                .build();
        log.trace("Created emptyDir Volume named '{}' with sizeLimit '{}'", name, sizeLimit);
        return volume;
    }

    /**
     * Creates a PVC template
     *
     * @param name    Name of the PVC template
     * @param storage Storage definition
     */
    public static PersistentVolumeClaim createPersistentVolumeClaimTemplate(String name, PersistentClaimStorage storage) {
        Map<String, Quantity> requests = new HashMap<>();
        requests.put("storage", new Quantity(storage.getSize(), null));

        LabelSelector selector = null;
        if (storage.getSelector() != null && !storage.getSelector().isEmpty()) {
            selector = new LabelSelector(null, storage.getSelector());
        }

        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withNewResources()
                .withRequests(requests)
                .endResources()
                .withStorageClassName(storage.getStorageClass())
                .withSelector(selector)
                .endSpec()
                .build();
    }

    /**
     * Creates a Volume mount
     *
     * @param name Name of the Volume mount
     * @param path volume mount path
     */
    public static VolumeMount createVolumeMount(String name, String path) {
        VolumeMount volumeMount = new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
        log.trace("Created volume mount {}", volumeMount);
        return volumeMount;
    }

    public static List<Volume> getDataVolumes(Storage storage) {
        List<Volume> volumes = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    if (volume.getId() == null)
                        throw new InvalidResourceException("Volumes under JBOD storage type have to have 'id' property");
                    // it's called recursively for setting the information from the current volume
                    volumes.addAll(getDataVolumes(volume));
                }
            } else if (storage instanceof EphemeralStorage) {
                Integer id = ((EphemeralStorage) storage).getId();
                String name = getVolumePrefix(id);
                String sizeLimit = ((EphemeralStorage) storage).getSizeLimit();
                volumes.add(createEmptyDirVolume(name, sizeLimit));
            }
        }

        return volumes;
    }

    public static List<PersistentVolumeClaim> getDataPersistentVolumeClaims(Storage storage) {
        List<PersistentVolumeClaim> pvcs = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    if (volume.getId() == null)
                        throw new InvalidResourceException("Volumes under JBOD storage type have to have 'id' property");
                    // it's called recursively for setting the information from the current volume
                    pvcs.addAll(getDataPersistentVolumeClaims(volume));
                }
            } else if (storage instanceof PersistentClaimStorage) {
                Integer id = ((PersistentClaimStorage) storage).getId();
                String name = getVolumePrefix(id);
                pvcs.add(createPersistentVolumeClaimTemplate(name, (PersistentClaimStorage) storage));
            }
        }

        return pvcs;
    }

    public static List<VolumeMount> getDataVolumeMountPaths(Storage storage, String mountPath) {
        List<VolumeMount> volumeMounts = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    if (volume.getId() == null)
                        throw new InvalidResourceException("Volumes under JBOD storage type have to have 'id' property");
                    // it's called recursively for setting the information from the current volume
                    volumeMounts.addAll(getDataVolumeMountPaths(volume, mountPath));
                }
            } else {
                Integer id;

                if (storage instanceof EphemeralStorage) {
                    id = ((EphemeralStorage) storage).getId();
                } else if (storage instanceof PersistentClaimStorage) {
                    id = ((PersistentClaimStorage) storage).getId();
                } else {
                    throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
                }

                String name = getVolumePrefix(id);
                String namedMountPath = mountPath + "/" + name;
                volumeMounts.add(createVolumeMount(name, namedMountPath));
            }
        }

        return volumeMounts;
    }

    /**
     * Returns the prefix used for volumes and persistent volume claims
     *
     * @param id identification number of the persistent storage
     * @return The volume prefix.
     */
    public static String getVolumePrefix(Integer id) {
        return id == null ? AbstractModel.VOLUME_NAME : AbstractModel.VOLUME_NAME + "-" + id;
    }
}
