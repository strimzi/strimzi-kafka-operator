/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
    private static Pattern volumeNamePattern = Pattern.compile("^([a-z0-9]{1}[a-z0-9-]{0,61}[a-z0-9]{1})$");

    /**
     * Creates a Kubernetes volume which will map to ConfigMap
     *
     * @param name              Name of the Volume
     * @param configMapName     Name of the ConfigMap
     * @return                  The newly created Volume
     */
    public static Volume createConfigMapVolume(String name, String configMapName) {
        String validName = getValidVolumeName(name);

        ConfigMapVolumeSource configMapVolumeSource = new ConfigMapVolumeSourceBuilder()
                .withName(configMapName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withConfigMap(configMapVolumeSource)
                .build();

        log.trace("Created configMap Volume named '{}' with source configMap '{}'", validName, configMapName);

        return volume;
    }

    /**
     * Creates a secret volume with given items
     *
     * @param name        Name of the Volume
     * @param secretName  Name of the Secret
     * @param items       contents of the Secret
     * @param isOpenshift true if underlying cluster OpenShift
     * @return The Volume created
     */
    public static Volume createSecretVolume(String name, String secretName, Map<String, String> items, boolean isOpenshift) {
        String validName = getValidVolumeName(name);

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
                .withName(validName)
                .withSecret(secretVolumeSource)
                .build();
        log.trace("Created secret Volume named '{}' with source secret '{}'", validName, secretName);
        return volume;
    }

    /**
     * Creates a secret volume
     *
     * @param name        Name of the Volume
     * @param secretName  Name of the Secret
     * @param isOpenshift true if underlying cluster OpenShift
     * @return The Volume created
     */
    public static Volume createSecretVolume(String name, String secretName, boolean isOpenshift) {
        String validName = getValidVolumeName(name);

        int mode = 0444;
        if (isOpenshift) {
            mode = 0440;
        }

        SecretVolumeSource secretVolumeSource = new SecretVolumeSourceBuilder()
                .withDefaultMode(mode)
                .withSecretName(secretName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withSecret(secretVolumeSource)
                .build();
        log.trace("Created secret Volume named '{}' with source secret '{}'", validName, secretName);
        return volume;
    }

    /**
     * Creates an empty directory volume
     *
     * @param name      Name of the Volume
     * @param sizeLimit Volume size
     * @return The Volume created
     */
    public static Volume createEmptyDirVolume(String name, String sizeLimit) {
        String validName = getValidVolumeName(name);

        EmptyDirVolumeSource emptyDirVolumeSource = new EmptyDirVolumeSourceBuilder().build();
        if (sizeLimit != null && !sizeLimit.isEmpty()) {
            emptyDirVolumeSource.setSizeLimit(new Quantity(sizeLimit));
        }

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withEmptyDir(emptyDirVolumeSource)
                .build();
        log.trace("Created emptyDir Volume named '{}' with sizeLimit '{}'", validName, sizeLimit);
        return volume;
    }

    /**
     * Creates a PVC template
     *
     * @param name    Name of the PVC template
     * @param storage Storage definition
     * @return The PVC created
     */
    public static PersistentVolumeClaim createPersistentVolumeClaimTemplate(String name, PersistentClaimStorage storage) {
        Map<String, Quantity> requests = new HashMap<>(1);
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
     * @return The Volume mount created
     */
    public static VolumeMount createVolumeMount(String name, String path) {
        String validName = getValidVolumeName(name);

        VolumeMount volumeMount = new VolumeMountBuilder()
                .withName(validName)
                .withMountPath(path)
                .build();
        log.trace("Created volume mount {} for volume {}", volumeMount, validName);
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

    /**
     * Volume names have to follow DNS label standard form RFC1123:
     *     - contain at most 63 characters
     *     - contain only lowercase alphanumeric characters or ‘-’
     *     - start with an alphanumeric character
     *     - end with an alphanumeric character
     *
     *  This method checkes if the volume name is a valid name and if not it will modify it to make it valid.
     *
     * @param originalName  The original name of the volume
     * @return              Either the original volume name or a modified version to match volume name criteria
     */
    public static String getValidVolumeName(String originalName) {
        if (originalName == null) {
            throw new RuntimeException("Volume name cannot be null");
        }

        if (volumeNamePattern.matcher(originalName).matches()) {
            return originalName;
        } else {
            return makeValidVolumeName(originalName);
        }
    }

    /**
     * Makes a valid volume name out of an invalid name. To do so it:
     *     - Replaces . and _ characters with -
     *     - Shortens the name if needed
     *     - Uses SHA1 hash for uniqueness of the new name
     *
     * @param originalName  Original invalid volume name
     * @return              New valid volume name
     */
    /*test*/ static String makeValidVolumeName(String originalName) {
        // SHA-1 hash is used for uniqueness
        String digestStub = getVolumeNameHashStub(originalName);

        // Special characters need to be replaced
        String newName = originalName
                .replace(".", "-")
                .replace("_", "-");

        // The name with the hash should be only up to 63 characters long
        int i = Math.min(newName.length(), 54);

        while (i > 0) {
            char lastChar = newName.charAt(i - 1);

            if (lastChar == '-') {
                i--;
            } else {
                break;
            }
        }

        // Returned new fixed name with the hash at the end
        return newName.substring(0, i) + "-" + digestStub;
    }

    /**
     * Gets the first 8 characters from a SHA-1 hash of a volume name
     *
     * @param name  Volume name
     * @return      First 8 characters of the SHA-1 hash
     */
    private static String getVolumeNameHashStub(String name)   {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] digest = sha1.digest(name.getBytes(StandardCharsets.US_ASCII));

            return String.format("%040x", new BigInteger(1, digest)).substring(0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get volume name SHA-1 hash", e);
        }
    }
}
