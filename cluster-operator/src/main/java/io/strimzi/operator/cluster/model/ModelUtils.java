/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ModelUtils is a utility class that holds generic static helper functions
 * These are generally to be used within the classes that extend the AbstractModel class
 */
public class ModelUtils {

    public static final io.strimzi.api.kafka.model.Probe DEFAULT_TLS_SIDECAR_PROBE = new io.strimzi.api.kafka.model.ProbeBuilder()
            .withInitialDelaySeconds(TlsSidecar.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(TlsSidecar.DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    private ModelUtils() {}

    protected static final Logger log = LogManager.getLogger(ModelUtils.class.getName());

    public static final String KUBERNETES_SERVICE_DNS_DOMAIN =
            System.getenv().getOrDefault("KUBERNETES_SERVICE_DNS_DOMAIN", "cluster.local");

    /**
     * Generates the DNS name of the pod including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-pod-1.my-service.my-ns.svc.cluster.local
     *
     * Note: Conventionally this would only be used for pods with deterministic names such as statefulset pods
     *
     * @param namespace     Namespace of the pod
     * @param serviceName   Name of the cluster
     * @param podName       Name of the pod
     *
     * @return              DNS name of the pod
     */
    public static String podDnsName(String namespace, String serviceName, String podName) {
        return String.format("%s.%s",
                podName,
                ModelUtils.serviceDnsName(namespace, serviceName));
    }

    /**
     * Generates the DNS name of the pod without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-cluster-pod-1.my-cluster-service.my-ns.svc
     *
     * Note: Conventionally this would only be used for pods with deterministic names such as statefulset pods
     *
     * @param namespace     Namespace of the pod
     * @param serviceName   Name of the service
     * @param podName       Name of the pod
     *
     * @return              DNS name of the pod without the cluster domain suffix
     */
    public static String podDnsNameWithoutClusterDomain(String namespace, String serviceName, String podName) {
        return String.format("%s.%s",
                podName,
                ModelUtils.serviceDnsNameWithoutClusterDomain(namespace, serviceName));

    }

    /**
     * Generates the DNS name of the service including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-service.my-ns.svc.cluster.local
     *
     * @param namespace     Namespace of the pod
     * @param serviceName   Name of the cluster
     *
     * @return              DNS name of the service
     */
    public static String serviceDnsName(String namespace, String serviceName) {
        return String.format("%s.%s.svc.%s",
                serviceName,
                namespace,
                ModelUtils.KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the wildcard DNS name of the service without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: *.my-service.my-ns.svc
     *
     * @param namespace     Namespace of the pod
     * @param serviceName   Name of the service
     *
     * @return              Wildcard DNS name of the service without the cluster domain suffix
     */
    public static String wildcardServiceDnsNameWithoutClusterDomain(String namespace, String serviceName) {
        return String.format("*.%s.%s.svc",
                serviceName,
                namespace);
    }

    /**
     * Generates the wildcard DNS name of the service including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: *.my-service.my-ns.svc.cluster.local
     *
     * @param namespace     Namespace of the pod
     * @param serviceName   Name of the cluster
     *
     * @return              Wildcard DNS name of the service
     */
    public static String wildcardServiceDnsName(String namespace, String serviceName) {
        return String.format("*.%s.%s.svc.%s",
                serviceName,
                namespace,
                ModelUtils.KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the DNS name of the service without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-service.my-ns.svc
     *
     * @param namespace     Namespace of the pod
     * @param serviceName   Name of the service
     *
     * @return              DNS name of the service without the cluster domain suffix
     */
    public static String serviceDnsNameWithoutClusterDomain(String namespace, String serviceName) {
        return String.format("%s.%s.svc",
                serviceName,
                namespace);
    }

    /**
     * @param certificateAuthority The CA configuration.
     * @return The cert validity.
     */
    public static int getCertificateValidity(CertificateAuthority certificateAuthority) {
        int validity = CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS;
        if (certificateAuthority != null
                && certificateAuthority.getValidityDays() > 0) {
            validity = certificateAuthority.getValidityDays();
        }
        return validity;
    }

    /**
     * @param certificateAuthority The CA configuration.
     * @return The renewal days.
     */
    public static int getRenewalDays(CertificateAuthority certificateAuthority) {
        int renewalDays = CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS;

        if (certificateAuthority != null
                && certificateAuthority.getRenewalDays() > 0) {
            renewalDays = certificateAuthority.getRenewalDays();
        }

        return renewalDays;
    }

    public static String formatTimestamp(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date);
    }

    /**
     * Generate labels used by entity-operators to find the resources related to given cluster
     *
     * @param cluster   Name of the cluster
     * @return  Map with label definition
     */
    public static String defaultResourceLabels(String cluster) {
        return String.format("%s=%s",
                Labels.STRIMZI_CLUSTER_LABEL, cluster);
    }

    /**
     * @param sts The StatefulSet
     * @param containerName The name of the container whoes environment variables are to be retrieved
     * @return The environment of the Kafka container in the sts.
     */
    public static Map<String, String> getContainerEnv(StatefulSet sts, String containerName) {
        for (Container container : sts.getSpec().getTemplate().getSpec().getContainers()) {
            if (containerName.equals(container.getName())) {
                LinkedHashMap<String, String> map = new LinkedHashMap<>(container.getEnv() == null ? 2 : container.getEnv().size());
                if (container.getEnv() != null) {
                    for (EnvVar envVar : container.getEnv()) {
                        map.put(envVar.getName(), envVar.getValue());
                    }
                }
                return map;
            }
        }
        throw new KafkaUpgradeException("Could not find '" + containerName + "' container in StatefulSet " + sts.getMetadata().getName());
    }

    /**
     * @param pod The StatefulSet
     * @param containerName The name of the container whoes environment variables are to be retrieved
     * @param envVarName Name of the environment variable which we should get
     * @return The environment of the Kafka container in the sts.
     */
    public static String getPodEnv(Pod pod, String containerName, String envVarName) {
        if (pod != null) {
            for (Container container : pod.getSpec().getContainers()) {
                if (containerName.equals(container.getName())) {
                    if (container.getEnv() != null) {
                        for (EnvVar envVar : container.getEnv()) {
                            if (envVarName.equals(envVar.getName()))    {
                                return envVar.getValue();
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    public static List<EnvVar> envAsList(Map<String, String> env) {
        ArrayList<EnvVar> result = new ArrayList<>(env.size());
        for (Map.Entry<String, String> entry : env.entrySet()) {
            result.add(new EnvVar(entry.getKey(), entry.getValue(), null));
        }
        return result;
    }

    protected static ProbeBuilder newProbeBuilder(io.strimzi.api.kafka.model.Probe userProbe) {
        return new ProbeBuilder()
                .withInitialDelaySeconds(userProbe.getInitialDelaySeconds())
                .withTimeoutSeconds(userProbe.getTimeoutSeconds())
                .withPeriodSeconds(userProbe.getPeriodSeconds())
                .withSuccessThreshold(userProbe.getSuccessThreshold())
                .withFailureThreshold(userProbe.getFailureThreshold());
    }


    protected static Probe createTcpSocketProbe(int port, io.strimzi.api.kafka.model.Probe userProbe) {
        Probe probe = ModelUtils.newProbeBuilder(userProbe)
                .withNewTcpSocket()
                .withNewPort()
                .withIntVal(port)
                .endPort()
                .endTcpSocket()
                .build();
        log.trace("Created TCP socket probe {}", probe);
        return probe;
    }

    protected static Probe createHttpProbe(String path, String port, io.strimzi.api.kafka.model.Probe userProbe) {
        Probe probe = ModelUtils.newProbeBuilder(userProbe).withNewHttpGet()
                .withPath(path)
                .withNewPort(port)
                .endHttpGet()
                .build();
        log.trace("Created http probe {}", probe);
        return probe;
    }

    static Probe createExecProbe(List<String> command, io.strimzi.api.kafka.model.Probe userProbe) {
        Probe probe = newProbeBuilder(userProbe).withNewExec()
                .withCommand(command)
                .endExec()
                .build();
        log.trace("Created exec probe {}", probe);
        return probe;
    }

    static Probe tlsSidecarReadinessProbe(TlsSidecar tlsSidecar) {
        io.strimzi.api.kafka.model.Probe tlsSidecarReadinessProbe;
        if (tlsSidecar != null && tlsSidecar.getReadinessProbe() != null) {
            tlsSidecarReadinessProbe = tlsSidecar.getReadinessProbe();
        } else {
            tlsSidecarReadinessProbe = DEFAULT_TLS_SIDECAR_PROBE;
        }
        return createExecProbe(Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"), tlsSidecarReadinessProbe);
    }

    static Probe tlsSidecarLivenessProbe(TlsSidecar tlsSidecar) {
        io.strimzi.api.kafka.model.Probe tlsSidecarLivenessProbe;
        if (tlsSidecar != null && tlsSidecar.getLivenessProbe() != null) {
            tlsSidecarLivenessProbe = tlsSidecar.getLivenessProbe();
        } else {
            tlsSidecarLivenessProbe = DEFAULT_TLS_SIDECAR_PROBE;
        }
        return createExecProbe(Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"), tlsSidecarLivenessProbe);
    }

    public static final String TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";

    static EnvVar tlsSidecarLogEnvVar(TlsSidecar tlsSidecar) {
        return AbstractModel.buildEnvVar(TLS_SIDECAR_LOG_LEVEL,
                (tlsSidecar != null && tlsSidecar.getLogLevel() != null ?
                        tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE).toValue());
    }

    public static Secret buildSecret(ClusterCa clusterCa, Secret secret, String namespace, String secretName,
            String commonName, String keyCertName, Labels labels, OwnerReference ownerReference, boolean isMaintenanceTimeWindowsSatisfied) {
        Map<String, String> data = new HashMap<>(4);
        CertAndKey certAndKey = null;
        boolean shouldBeRegenerated = false;
        List<String> reasons = new ArrayList<>(2);

        if (secret == null) {
            reasons.add("certificate doesn't exist yet");
            shouldBeRegenerated = true;
        } else {
            if (clusterCa.keyCreated() || clusterCa.certRenewed() || (isMaintenanceTimeWindowsSatisfied && clusterCa.isExpiring(secret, keyCertName + ".crt"))) {
                reasons.add("certificate needs to be renewed");
                shouldBeRegenerated = true;
            }
        }

        if (shouldBeRegenerated) {
            log.debug("Certificate for pod {} need to be regenerated because: {}", keyCertName, String.join(", ", reasons));

            try {
                certAndKey = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
            } catch (IOException e) {
                log.warn("Error while generating certificates", e);
            }

            log.debug("End generating certificates");
        } else {
            if (secret.getData().get(keyCertName + ".p12") != null &&
                    !secret.getData().get(keyCertName + ".p12").isEmpty() &&
                    secret.getData().get(keyCertName + ".password") != null &&
                    !secret.getData().get(keyCertName + ".password").isEmpty()) {
                certAndKey = new CertAndKey(
                        decodeFromSecret(secret, keyCertName + ".key"),
                        decodeFromSecret(secret, keyCertName + ".crt"),
                        null,
                        decodeFromSecret(secret, keyCertName + ".p12"),
                        new String(decodeFromSecret(secret, keyCertName + ".password"), StandardCharsets.US_ASCII)
                );
            } else {
                try {
                    // coming from an older operator version, the secret exists but without keystore and password
                    certAndKey = clusterCa.addKeyAndCertToKeyStore(commonName,
                            decodeFromSecret(secret, keyCertName + ".key"),
                            decodeFromSecret(secret, keyCertName + ".crt"));
                } catch (IOException e) {
                    log.error("Error generating the keystore for {}", keyCertName, e);
                }
            }
        }

        if (certAndKey != null) {
            data.put(keyCertName + ".key", certAndKey.keyAsBase64String());
            data.put(keyCertName + ".crt", certAndKey.certAsBase64String());
            data.put(keyCertName + ".p12", certAndKey.keyStoreAsBase64String());
            data.put(keyCertName + ".password", certAndKey.storePasswordAsBase64String());
        }

        return createSecret(secretName, namespace, labels, ownerReference, data);
    }

    public static Secret createSecret(String name, String namespace, Labels labels, OwnerReference ownerReference, Map<String, String> data) {
        if (ownerReference == null) {
            return new SecretBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .endMetadata()
                    .withData(data).build();
        } else {
            return new SecretBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withOwnerReferences(ownerReference)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .endMetadata()
                    .withData(data).build();
        }
    }

    /**
     * Parses the values from the PodDisruptionBudgetTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodDisruptionBudgetTemplate should be set
     * @param pdb PodDisruptionBudgetTemplate with the values form the CRD
     */
    public static void parsePodDisruptionBudgetTemplate(AbstractModel model, PodDisruptionBudgetTemplate pdb)   {
        if (pdb != null)  {
            if (pdb.getMetadata() != null) {
                model.templatePodDisruptionBudgetLabels = pdb.getMetadata().getLabels();
                model.templatePodDisruptionBudgetAnnotations = pdb.getMetadata().getAnnotations();
            }

            model.templatePodDisruptionBudgetMaxUnavailable = pdb.getMaxUnavailable();
        }
    }

    /**
     * Parses the values from the PodTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodTemplate should be set
     * @param pod PodTemplate with the values form the CRD
     */

    public static void parsePodTemplate(AbstractModel model, PodTemplate pod)   {
        if (pod != null)  {
            if (pod.getMetadata() != null) {
                model.templatePodLabels = pod.getMetadata().getLabels();
                model.templatePodAnnotations = pod.getMetadata().getAnnotations();
            }

            if (pod.getAffinity() != null)  {
                model.setUserAffinity(pod.getAffinity());
            }

            if (pod.getTolerations() != null)   {
                model.setTolerations(pod.getTolerations());
            }

            model.templateTerminationGracePeriodSeconds = pod.getTerminationGracePeriodSeconds();
            model.templateImagePullSecrets = pod.getImagePullSecrets();
            model.templateSecurityContext = pod.getSecurityContext();
            model.templatePodPriorityClassName = pod.getPriorityClassName();
            model.templatePodSchedulerName = pod.getSchedulerName();
        }
    }

    /**
     * Returns whether the given {@code Storage} instance is a persistent claim one or
     * a JBOD containing at least one persistent volume.
     *
     * @param storage the Storage instance to check
     * @return Whether the give Storage contains any persistent storage.
     */
    public static boolean containsPersistentStorage(Storage storage) {
        boolean isPersistentClaimStorage = storage instanceof PersistentClaimStorage;

        if (!isPersistentClaimStorage && storage instanceof JbodStorage) {
            isPersistentClaimStorage |= ((JbodStorage) storage).getVolumes()
                    .stream().anyMatch(volume -> volume instanceof PersistentClaimStorage);
        }
        return isPersistentClaimStorage;
    }

    public static Storage decodeStorageFromJson(String json) {
        try {
            return new ObjectMapper().readValue(json, Storage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String encodeStorageToJson(Storage storage) {
        try {
            return new ObjectMapper().writeValueAsString(storage);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a map with custom labels or annotations from an environment variable
     *
     * @param envVarName Name of the environment variable which should be used as input
     *
     * @return A map with labels or annotations
     */
    public static Map<String, String> getCustomLabelsOrAnnotations(String envVarName)   {
        return Util.parseMap(System.getenv().get(envVarName));
    }

    private static byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }

    /**
     * Compares two Secrets with certificates and checks whether any value for a key which exists in both Secrets
     * changed. This method is used to evaluate whether rolling update of existing brokers is needed when secrets with
     * certificates change. It separates changes for existing certificates with other changes to the secret such as
     * added or removed certificates (scale-up or scale-down).
     *
     * @param current   Existing secret
     * @param desired   Desired secret
     *
     * @return  True if there is a key which exists in the data sections of both secrets and which changed.
     */
    public static boolean doExistingCertificatesDiffer(Secret current, Secret desired) {
        Map<String, String> currentData = current.getData();
        Map<String, String> desiredData = desired.getData();

        for (Map.Entry<String, String> entry : currentData.entrySet()) {
            String desiredValue = desiredData.get(entry.getKey());
            if (entry.getValue() != null
                    && desiredValue != null
                    && !entry.getValue().equals(desiredValue)) {
                return true;
            }
        }

        return false;
    }

    public static <T> List<T> asListOrEmptyList(List<T> list) {
        return Optional.ofNullable(list)
                .orElse(Collections.emptyList());
    }

    public static String getJavaSystemPropertiesToString(List<SystemProperty> javaSystemProperties) {
        if (javaSystemProperties == null) {
            return null;
        }
        List<String> javaSystemPropertiesList = new ArrayList<>(javaSystemProperties.size());
        for (SystemProperty property: javaSystemProperties) {
            javaSystemPropertiesList.add("-D" + property.getName() + "=" + property.getValue());
        }
        return String.join(" ", javaSystemPropertiesList);
    }

    /**
     * This method transforms a String into a List of Strings, where each entry is an uncommented line of input.
     * The lines beginning with '#' (comments) are ignored.
     * @param config ConfigMap data as a String
     * @return List of String key=value
     */
    public static List<String> getLinesWithoutCommentsAndEmptyLines(String config) {
        List<String> validLines = new ArrayList<>();
        if (config != null) {
            List<String> allLines = Arrays.asList(config.split("\\r?\\n"));

            for (String line : allLines) {
                if (!line.isEmpty() && !line.matches("\\s*\\#.*")) {
                    validLines.add(line);
                }
            }
        }
        return validLines;
    }

    /**
     * If the toleration.value is an empty string, set it to null. That solves an issue when built STS contains a filed
     * with an empty property value. K8s is removing properties like this and thus we cannot fetch an equal STS which was
     * created with (some) empty value.
     * @param tolerations tolerations list to check whether toleration.value is an empty string and eventually replace it by null
     */
    public static void removeEmptyValuesFromTolerations(List<Toleration> tolerations) {
        if (tolerations != null) {
            tolerations.stream().filter(toleration -> toleration.getValue() != null && toleration.getValue().isEmpty()).forEach(emptyValTol -> emptyValTol.setValue(null));
        }
    }

    /**
     * Checks whether tolerations and template.tolerations exits. If so, latter takes precedence. Entries like tolerations.value == ""
     * are replaced by tolerations.value = null
     * @param tolerations path to tolerations in CR
     * @param tolerationList tolerations
     * @param templateTolerations path to template.tolerations in CR
     * @param podTemplate pod template containing tolerations
     * @return adjusted list with tolerations
     */
    public static List<Toleration> tolerations(String tolerations, List<Toleration> tolerationList, String templateTolerations, PodTemplate podTemplate) {
        List<Toleration> tolerationsListLocal;
        if (podTemplate != null && podTemplate.getTolerations() != null) {
            if (tolerationList != null) {
                log.warn("Tolerations given on both {} and {}; latter takes precedence", tolerations, templateTolerations);
            }
            tolerationsListLocal = podTemplate.getTolerations();
        } else {
            tolerationsListLocal = tolerationList;
        }
        removeEmptyValuesFromTolerations(tolerationsListLocal);
        return tolerationsListLocal;
    }
}
