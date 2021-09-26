/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;

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
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverride;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared methods for working with Volume
 */
public class VolumeUtils {
    private static final Logger LOGGER = LogManager.getLogger(VolumeUtils.class.getName());
    private static Pattern volumeNamePattern = Pattern.compile("^([a-z0-9]{1}[a-z0-9-]{0,61}[a-z0-9]{1})$");

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

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withConfigMap(configMapVolumeSource)
                .build();

        return volume;
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

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withConfigMap(configMapVolumeSource)
                .build();

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

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withSecret(secretVolumeSource)
                .build();
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
        return volume;
    }

    /**
     * Creates an empty directory volume
     *
     * @param name      Name of the Volume
     * @param sizeLimit Volume size
     * @param medium    Medium used for the emptryDir
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

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withEmptyDir(emptyDirVolumeSource)
                .build();
        return volume;
    }

    public static Volume createPersistentVolume(String name, String claimName) {
        String validName = getValidVolumeName(name);

        PersistentVolumeClaimVolumeSource persistentVolumeSource = new PersistentVolumeClaimVolumeSourceBuilder()
                .withClaimName(claimName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(validName)
                .withPersistentVolumeClaim(persistentVolumeSource)
                .build();

        LOGGER.trace("Created persistent Volume named '{}' with source claim '{}'", validName, claimName);

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
                volumes.add(createEmptyDirVolume(name, sizeLimit, null));
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
                if (storage instanceof PersistentClaimStorage) {
                    List<PersistentClaimStorageOverride> overrides = ((PersistentClaimStorage) storage).getOverrides();
                    int id1 = id == null ? 0 : id;
                    if (overrides != null && overrides.size() > id1) {
                        String mountPathOverride = overrides.get(id1).getMountPath();
                        if (mountPathOverride != null) {
                            namedMountPath = mountPathOverride;
                        }
                    }
                }
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

    /**
     * Creates the Client Secret Volume
     * @param volumeList    List where the volumes will be added
     * @param trustedCertificates   Trusted certificates for TLS connection
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     */
    public static void createSecretVolume(List<Volume> volumeList, List<CertSecretSource> trustedCertificates, boolean isOpenShift) {
        createSecretVolume(volumeList, trustedCertificates, isOpenShift, null);
    }

    /**
     * Creates the Client Secret Volume
     * @param volumeList    List where the volumes will be added
     * @param trustedCertificates   Trusted certificates for TLS connection
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     * @param alias   Alias to reference the Kafka Cluster
     */
    public static void createSecretVolume(List<Volume> volumeList, List<CertSecretSource> trustedCertificates, boolean isOpenShift, String alias) {

        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            for (CertSecretSource certSecretSource : trustedCertificates) {
                addSecretVolume(volumeList, certSecretSource, isOpenShift, alias);
            }
        }
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components, checking that the named volume has not already been
     * created.
     *
     * @param volumeList    List where the volume will be added
     * @param certSecretSource   Represents a certificate inside a Secret
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     * @param alias   Alias to reference the Kafka Cluster
     */
    private static void addSecretVolume(List<Volume> volumeList, CertSecretSource certSecretSource, boolean isOpenShift, String alias) {
        String volumeName = alias != null ? alias + '-' + certSecretSource.getSecretName() : certSecretSource.getSecretName();
        // skipping if a volume with same name was already added
        if (!volumeList.stream().anyMatch(v -> v.getName().equals(volumeName))) {
            volumeList.add(VolumeUtils.createSecretVolume(volumeName, certSecretSource.getSecretName(), isOpenShift));
        }
    }

    /**
     * Creates the Client tls encrypted Volume Mounts
     *  @param volumeMountList    List where the volume mounts will be added
     * @param trustedCertificates   Trusted certificates for TLS connection
     * @param tlsVolumeMountPath   Path where the TLS certs should be mounted
     */
    public static void createSecretVolumeMount(List<VolumeMount> volumeMountList, List<CertSecretSource> trustedCertificates, String tlsVolumeMountPath) {
        createSecretVolumeMount(volumeMountList, trustedCertificates, tlsVolumeMountPath, null);
    }

    /**
     * Creates the Client Tls encrypted Volume Mount
     *
     * @param volumeMountList    List where the volume mounts will be added
     * @param trustedCertificates  Trusted certificates for TLS connection
     * @param tlsVolumeMountPath  Path where the TLS certs should be mounted
     * @param alias   Alias to reference the Kafka Cluster
     */
    public static void createSecretVolumeMount(List<VolumeMount> volumeMountList, List<CertSecretSource> trustedCertificates, String tlsVolumeMountPath, String alias) {

        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            for (CertSecretSource certSecretSource : trustedCertificates) {
                addSecretVolumeMount(volumeMountList, certSecretSource, tlsVolumeMountPath, alias);
            }
        }
    }

    /**
     * Creates the VolumeMount used for authentication of Kafka client based components, checking that the named volume mount has not already been
     * created.
     *
     * @param volumeMountList    List where the volume mount will be added
     * @param certSecretSource   Represents a certificate inside a Secret
     * @param tlsVolumeMountPath   Path where the TLS certs should be mounted
     * @param alias   Alias to reference the Kafka Cluster
     */
    private static void addSecretVolumeMount(List<VolumeMount> volumeMountList,  CertSecretSource certSecretSource, String tlsVolumeMountPath, String alias) {
        String volumeMountName = alias != null ? alias + '-' + certSecretSource.getSecretName() : certSecretSource.getSecretName();
        // skipping if a volume mount with same Secret name was already added
        if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(volumeMountName))) {
            volumeMountList.add(createVolumeMount(volumeMountName,
                    tlsVolumeMountPath + certSecretSource.getSecretName()));
        }
    }
}
