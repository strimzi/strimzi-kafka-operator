/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CpuMemory;
import io.strimzi.api.kafka.model.JbodStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static io.strimzi.api.kafka.model.Quantities.normalizeCpu;
import static io.strimzi.api.kafka.model.Quantities.normalizeMemory;

public class ModelUtils {
    private ModelUtils() {}

    protected static final Logger log = LogManager.getLogger(ModelUtils.class.getName());

    public static final String KUBERNETES_SERVICE_DNS_DOMAIN =
            System.getenv().getOrDefault("KUBERNETES_SERVICE_DNS_DOMAIN", "cluster.local");

    /**
     * Find the first secret in the given secrets with the given name
     */
    public static Secret findSecretWithName(List<Secret> secrets, String sname) {
        return secrets.stream().filter(s -> s.getMetadata().getName().equals(sname)).findFirst().orElse(null);
    }

    public static int getCertificateValidity(CertificateAuthority certificateAuthority) {
        int validity = CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS;
        if (certificateAuthority != null
                && certificateAuthority.getValidityDays() > 0) {
            validity = certificateAuthority.getValidityDays();
        }
        return validity;
    }

    public static int getRenewalDays(CertificateAuthority certificateAuthority) {
        return certificateAuthority != null ? certificateAuthority.getRenewalDays() : CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS;
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
     * <p>Parse an image map. It has the structure:</p>
     * <pre><code>
     * imageMap ::= versionImage ( ',' versionImage )*
     * versionImage ::= version '=' image
     * version ::= [0-9.]+
     * image ::= [, \t\r\n]+
     * </code></pre>
     * For example {@code 2.0.0=strimzi/kafka:latest-kafka-2.0.0, 2.1.0=strimzi/kafka:latest-kafka-2.1.0}.
     * @param str
     * @return
     */
    public static Map<String, String> parseImageMap(String str) {
        if (str != null) {
            StringTokenizer tok = new StringTokenizer(str, ", \t\n\r");
            HashMap<String, String> map = new HashMap<>();
            while (tok.hasMoreTokens()) {
                String versionImage = tok.nextToken();
                int endIndex = versionImage.indexOf('=');
                String version = versionImage.substring(0, endIndex);
                String image = versionImage.substring(endIndex + 1);
                map.put(version.trim(), image.trim());
            }
            return Collections.unmodifiableMap(map);
        } else {
            return Collections.emptyMap();
        }
    }

    public static Map<String, String> getKafkaContainerEnv(StatefulSet ss) {
        for (Container container : ss.getSpec().getTemplate().getSpec().getContainers()) {
            if ("kafka".equals(container.getName())) {
                LinkedHashMap<String, String> map = new LinkedHashMap<>(container.getEnv() == null ? 2 : container.getEnv().size());
                if (container.getEnv() != null) {
                    for (EnvVar envVar : container.getEnv()) {
                        map.put(envVar.getName(), envVar.getValue());
                    }
                }
                return map;
            }
        }
        throw new KafkaUpgradeException("Could not find 'kafka' container in StatefulSet " + ss.getMetadata().getName());
    }

    public static List<EnvVar> envAsList(Map<String, String> env) {
        ArrayList<EnvVar> result = new ArrayList<>(env.size());
        for (Map.Entry<String, String> entry : env.entrySet()) {
            result.add(new EnvVar(entry.getKey(), entry.getValue(), null));
        }
        return result;
    }

    static Probe createExecProbe(List<String> command, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        AbstractModel.log.trace("Created exec probe {}", probe);
        return probe;
    }

    static Probe tlsSidecarReadinessProbe(TlsSidecar tlsSidecar) {
        int tlsSidecarReadinessInitialDelay = TlsSidecar.DEFAULT_HEALTHCHECK_DELAY;
        int tlsSidecarReadinessTimeout = TlsSidecar.DEFAULT_HEALTHCHECK_TIMEOUT;
        if (tlsSidecar != null && tlsSidecar.getReadinessProbe() != null) {
            tlsSidecarReadinessInitialDelay = tlsSidecar.getReadinessProbe().getInitialDelaySeconds();
            tlsSidecarReadinessTimeout = tlsSidecar.getReadinessProbe().getTimeoutSeconds();
        }
        return createExecProbe(Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"), tlsSidecarReadinessInitialDelay, tlsSidecarReadinessTimeout);
    }

    static Probe tlsSidecarLivenessProbe(TlsSidecar tlsSidecar) {
        int tlsSidecarLivenessInitialDelay = TlsSidecar.DEFAULT_HEALTHCHECK_DELAY;
        int tlsSidecarLivenessTimeout = TlsSidecar.DEFAULT_HEALTHCHECK_TIMEOUT;
        if (tlsSidecar != null && tlsSidecar.getLivenessProbe() != null) {
            tlsSidecarLivenessInitialDelay = tlsSidecar.getLivenessProbe().getInitialDelaySeconds();
            tlsSidecarLivenessTimeout = tlsSidecar.getLivenessProbe().getTimeoutSeconds();
        }
        return createExecProbe(Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"), tlsSidecarLivenessInitialDelay, tlsSidecarLivenessTimeout);
    }

    static ResourceRequirements resources(Resources resources) {
        if (resources != null) {
            ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder();
            CpuMemory limits = resources.getLimits();
            if (limits != null
                    && limits.milliCpuAsInt() > 0) {
                builder.addToLimits("cpu", new Quantity(normalizeCpu(limits.getMilliCpu())));
            }
            if (limits != null
                    && limits.memoryAsLong() > 0) {
                builder.addToLimits("memory", new Quantity(normalizeMemory(limits.getMemory())));
            }
            CpuMemory requests = resources.getRequests();
            if (requests != null
                    && requests.milliCpuAsInt() > 0) {
                builder.addToRequests("cpu", new Quantity(normalizeCpu(requests.getMilliCpu())));
            }
            if (requests != null
                    && requests.memoryAsLong() > 0) {
                builder.addToRequests("memory", new Quantity(normalizeMemory(requests.getMemory())));
            }
            return builder.build();
        }
        return null;
    }

    static ResourceRequirements tlsSidecarResources(TlsSidecar tlsSidecar) {
        return resources(tlsSidecar != null ? tlsSidecar.getResources() : null);
    }

    public static final String TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";

    static EnvVar tlsSidecarLogEnvVar(TlsSidecar tlsSidecar) {
        return AbstractModel.buildEnvVar(TLS_SIDECAR_LOG_LEVEL,
                (tlsSidecar != null && tlsSidecar.getLogLevel() != null ?
                        tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE).toValue());
    }

    public static Secret buildSecret(ClusterCa clusterCa, Secret secret, String namespace, String secretName, String commonName, String keyCertName, Labels labels, OwnerReference ownerReference) {
        Map<String, String> data = new HashMap<>();
        if (secret == null || clusterCa.certRenewed()) {
            log.debug("Generating certificates");
            try {
                log.debug(keyCertName + " certificate to generate");
                CertAndKey eoCertAndKey = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
                data.put(keyCertName + ".key", eoCertAndKey.keyAsBase64String());
                data.put(keyCertName + ".crt", eoCertAndKey.certAsBase64String());
            } catch (IOException e) {
                log.warn("Error while generating certificates", e);
            }
            log.debug("End generating certificates");
        } else {
            data.put(keyCertName + ".key", secret.getData().get(keyCertName + ".key"));
            data.put(keyCertName + ".crt", secret.getData().get(keyCertName + ".crt"));
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

            model.templateTerminationGracePeriodSeconds = pod.getTerminationGracePeriodSeconds();
            model.templateImagePullSecrets = pod.getImagePullSecrets();
            model.templateSecurityContext = pod.getSecurityContext();
        }
    }

    /**
     * Returns whether the given {@code Storage} instance is a persistent claim one or
     * a JBOD containing at least one persistent volume.
     *
     * @param storage the Storage instance to check
     */
    public static boolean containsPersistentStorage(Storage storage) {
        boolean isPersistentClaimStorage = storage instanceof PersistentClaimStorage;

        if (!isPersistentClaimStorage && storage instanceof JbodStorage) {
            isPersistentClaimStorage |= ((JbodStorage) storage).getVolumes()
                    .stream().anyMatch(volume -> volume instanceof PersistentClaimStorage);
        }
        return isPersistentClaimStorage;
    }

    /**
     * Returns the prefix used for volumes and persistent volume claims
     *
     * @param id identification number of the persistent storage
     */
    public static String getVolumePrefix(Integer id) {
        return id == null ? AbstractModel.VOLUME_NAME : AbstractModel.VOLUME_NAME + "-" + id;
    }
}
