/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Shared methods for working with Volume
 */
public class VolumeUtils {
    /**
     * Base name used to name data volumes
     */
    public static final String DATA_VOLUME_NAME = "data";

    /**
     * The path where the Kafka data volumes are mounted
     */
    private static final String KAFKA_DATA_VOLUME_MOUNT_PATH = "/var/lib/kafka";

    /*
     * Default values for the Strimzi temporary directory
     */
    /*test*/ static final String STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-tmp";
    /*test*/ static final String STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH = "/tmp";
    /*test*/ static final String STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE = "5Mi";

    private static final Pattern VOLUME_NAME_PATTERN = Pattern.compile("^([a-z0-9]{1}[a-z0-9-]{0,61}[a-z0-9]{1})$");

    private VolumeUtils() { }

    /**
     * Creates a Kubernetes volume which will map to ConfigMap with specific items mounted
     *
     * @param name              Name of the Volume
     * @param configMapName     Name of the ConfigMap
     * @param items             Specific items which should be mapped from the ConfigMap
     * @return                  The newly created Volume
     */
    public static Volume createConfigMapVolume(String name, String configMapName, Map<String, String> items) {
        String validName = getValidVolumeName(name);

        List<KeyToPath> keysPaths = new ArrayList<>();

        for (Map.Entry<String, String> item : items.entrySet()) {
            KeyToPath keyPath = new KeyToPathBuilder()
                    .withKey(item.getKey())
                    .withPath(item.getValue())
                    .build();

            keysPaths.add(keyPath);
        }

        ConfigMapVolumeSource configMapVolumeSource = new ConfigMapVolumeSourceBuilder()
                .withName(configMapName)
                .withItems(keysPaths)
                .build();

        return new VolumeBuilder()
                .withName(validName)
                .withConfigMap(configMapVolumeSource)
                .build();
    }

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

        return new VolumeBuilder()
                .withName(validName)
                .withConfigMap(configMapVolumeSource)
                .build();
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
                    .withKey(item.getKey())
                    .withPath(item.getValue())
                    .build();

            keysPaths.add(keyPath);
        }

        SecretVolumeSource secretVolumeSource = new SecretVolumeSourceBuilder()
                .withDefaultMode(mode)
                .withSecretName(secretName)
                .withItems(keysPaths)
                .build();

        return new VolumeBuilder()
                .withName(validName)
                .withSecret(secretVolumeSource)
                .build();
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

        return  new VolumeBuilder()
                .withName(validName)
                .withSecret(secretVolumeSource)
                .build();
    }

    /**
     * Creates an empty directory volume
     *
     * @param name      Name of the Volume
     * @param sizeLimit Volume size
     * @param medium    Medium used for the emptyDir
     *
     * @return The Volume created
     */
    public static Volume createEmptyDirVolume(String name, String sizeLimit, String medium) {
        String validName = getValidVolumeName(name);

        EmptyDirVolumeSource emptyDirVolumeSource = new EmptyDirVolumeSourceBuilder().build();

        if (sizeLimit != null && !sizeLimit.isEmpty()) {
            emptyDirVolumeSource.setSizeLimit(new Quantity(sizeLimit));
        }

        if (medium != null) {
            emptyDirVolumeSource.setMedium(medium);
        }

        return new VolumeBuilder()
                .withName(validName)
                .withEmptyDir(emptyDirVolumeSource)
                .build();
    }

    /**
     * Creates a volume for the temp directory with the default name
     *
     * @param template  Pod template which might contain a custom configuration for the size of the temp directory
     *
     * @return  Temp directory volume
     */
    public static Volume createTempDirVolume(PodTemplate template) {
        return createTempDirVolume(STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, template);
    }

    /**
     * Creates a volume for the temp directory with custom name. The custom volume is important when running multiple
     * containers in a single pod which need different volume names each.
     *
     * @param volumeName    Name of the volume
     * @param template      Pod template which might contain a custom configuration for the size of the temp directory
     *
     * @return  Temp directory volume
     */
    public static Volume createTempDirVolume(String volumeName, PodTemplate template) {
        return VolumeUtils.createEmptyDirVolume(volumeName, template != null && template.getTmpDirSizeLimit() != null ? template.getTmpDirSizeLimit() : STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE, "Memory");
    }

    /**
     * Creates a volume referencing a PVC
     *
     * @param name      Name of the Volume
     * @param pvcName   Name of the PVC
     *
     * @return The created Volume with the PersistentVolumeClaimSource
     */
    public static Volume createPvcVolume(String name, String pvcName) {
        String validName = getValidVolumeName(name);

        return new VolumeBuilder()
                .withName(validName)
                .withNewPersistentVolumeClaim()
                    .withClaimName(pvcName)
                .endPersistentVolumeClaim()
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

        return new VolumeMountBuilder()
                .withName(validName)
                .withMountPath(path)
                .build();
    }

    /**
     * Creates the volume mount for the temp directory with a default volume name
     *
     * @return  Volume mount for the temp directory
     */
    public static VolumeMount createTempDirVolumeMount() {
        return createTempDirVolumeMount(STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME);
    }

    /**
     * Creates the volume mount for the temp directory with a custom volume name. This is useful when running multiple
     * containers in the same pod and needing to use different volume name for each of them.
     *
     * @param volumeName    Name of the volume
     *
     * @return  Volume mount for the temp directory
     */
    public static VolumeMount createTempDirVolumeMount(String volumeName) {
        return createVolumeMount(volumeName, STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH);
    }

    /**
     * Generates the list of data volumes as used in PodSets and individual Pods. This includes both ephemeral and
     * persistent data volumes. This method calls itself recursively to create the volumes from a JBOD storage array.
     * When it does so, it sets the {@code jbod} parameter to {@code true}. When called from outside, it should be set
     * to {@code false}.
     *
     * @param podName   Name of the pod used to name the volumes
     * @param storage   Storage configuration
     * @param jbod      Indicates that the storage is part of JBOD storage and volume names are created accordingly
     *
     * @return          List of data volumes to be included in the StrimziPodSet pod
     */
    public static List<Volume> createPodSetVolumes(String podName, Storage storage, boolean jbod) {
        List<Volume> volumes = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    // it's called recursively for setting the information from the current volume
                    volumes.addAll(createPodSetVolumes(podName, volume, true));
                }
            } else if (storage instanceof EphemeralStorage ephemeralStorage) {
                volumes.add(
                        createEmptyDirVolume(
                                createVolumePrefix(ephemeralStorage.getId(), jbod),
                                ephemeralStorage.getSizeLimit(),
                                null
                        )
                );
            } else if (storage instanceof PersistentClaimStorage)   {
                String name = createVolumePrefix(((PersistentClaimStorage) storage).getId(), jbod);
                volumes.add(createPvcVolume(name, name + "-" + podName));
            }
        }

        return volumes;
    }

    /**
     * Creates list of volume mounts used by a Pod. This method calls itself recursively to handle volumes inside JBOD
     * storage. When it calls itself to handle the volumes inside JBOD array, the {@code jbod} flag should be set to
     * {@code true}. When called from outside, it should be set to {@code false}.
     *
     * @param storage   The storage configuration
     * @param jbod      Indicator whether the {@code storage} is part of JBOD array or not
     *
     * @return          List with Persistent Volume Claims templates
     */
    public static List<VolumeMount> createVolumeMounts(Storage storage, boolean jbod) {
        List<VolumeMount> volumeMounts = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    // it's called recursively for setting the information from the current volume
                    volumeMounts.addAll(createVolumeMounts(volume, true));
                }
            } else if (storage instanceof SingleVolumeStorage) {
                String name = createVolumePrefix(((SingleVolumeStorage) storage).getId(), jbod);
                String namedMountPath = KAFKA_DATA_VOLUME_MOUNT_PATH + "/" + name;
                volumeMounts.add(createVolumeMount(name, namedMountPath));
            }
        }

        return volumeMounts;
    }

    /**
     * Returns the prefix used for volumes and persistent volume claims. Volumes in JBOD storage contain ID in the name.
     * Volumes outside the JBOD storage do not.
     *
     * @param id    ID of the volume used within JBOD storage.
     * @param jbod  Indicates whether the volume is part of the JBOD storage or not.
     *
     * @return      The volume prefix.
     */
    public static String createVolumePrefix(Integer id, boolean jbod) {
        if (jbod) {
            if (id == null) {
                throw new InvalidResourceException("The 'id' property is required for volumes in JBOD storage.");
            }

            return DATA_VOLUME_NAME + "-" + id;
        } else {
            return DATA_VOLUME_NAME;
        }
    }

    /**
     * Volume names have to follow DNS label standard form RFC1123:
     *     - contain at most 63 characters
     *     - contain only lowercase alphanumeric characters or ‘-’
     *     - start with an alphanumeric character
     *     - end with an alphanumeric character
     *
     *  This method checks if the volume name is a valid name and if not it will modify it to make it valid.
     *
     * @param originalName  The original name of the volume
     * @return              Either the original volume name or a modified version to match volume name criteria
     */
    public static String getValidVolumeName(String originalName) {
        if (originalName == null) {
            throw new RuntimeException("Volume name cannot be null");
        }

        if (VOLUME_NAME_PATTERN.matcher(originalName).matches()) {
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
        String digestStub = Util.hashStub(originalName);

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
     * Creates the mount path of the volume where the KRaft metadata should be stored. This is either the volume marked
     * for KRaft metadata or the volume with the lowest available ID. The actual metadata will be stored in a
     * subdirectory of this volume. The exact name of the subdirectory depends on the node ID.
     *
     * @param storage       Storage configuration
     *
     * @return  Mount path of the volume where the KRaft metadata will be stored
     */
    protected static String kraftMetadataPath(Storage storage)  {
        if (storage instanceof JbodStorage jbodStorage) {
            SingleVolumeStorage kraftMetadataVolume = jbodStorage.getVolumes()
                    .stream()
                    .filter(v -> KRaftMetadataStorage.SHARED.equals(v.getKraftMetadata()))
                    .findFirst()
                    .orElse(jbodStorage.getVolumes().stream().min(Comparator.comparing(SingleVolumeStorage::getId)).orElse(null));

            if (kraftMetadataVolume != null)    {
                String name = createVolumePrefix(kraftMetadataVolume.getId(), true);
                return KAFKA_DATA_VOLUME_MOUNT_PATH + "/" + name;
            } else {
                throw new InvalidResourceException("Cannot find any data volumes for storing KRaft metadata.");
            }
        } else if (storage instanceof SingleVolumeStorage singleVolumeStorage)  {
            String name = createVolumePrefix(singleVolumeStorage.getId(), false);
            return KAFKA_DATA_VOLUME_MOUNT_PATH + "/" + name;
        } else {
            throw new InvalidResourceException("Cannot find any data volumes for storing KRaft metadata.");
        }
    }
}