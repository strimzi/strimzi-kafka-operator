/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * ModelUtils is a utility class that holds generic static helper functions
 * These are generally to be used within the classes that extend the AbstractModel class
 */
public class ModelUtils {

    private ModelUtils() {}

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ModelUtils.class.getName());
    protected static final String TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";

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

    static EnvVar tlsSidecarLogEnvVar(TlsSidecar tlsSidecar) {
        return ContainerUtils.createEnvVar(TLS_SIDECAR_LOG_LEVEL,
                (tlsSidecar != null && tlsSidecar.getLogLevel() != null ?
                        tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE).toValue());
    }

    /**
     * Builds a certificate secret for the different Strimzi components (TO, UO, KE, ...)
     *
     * @param reconciliation                        Reconciliation marker
     * @param clusterCa                             The Cluster CA
     * @param secret                                Kubernetes Secret
     * @param namespace                             Namespace
     * @param secretName                            Name of the Kubernetes secret
     * @param commonName                            Common Name of the certificate
     * @param keyCertName                           Key under which the certificate will be stored in the new Secret
     * @param labels                                Labels
     * @param ownerReference                        Owner reference
     * @param isMaintenanceTimeWindowsSatisfied     Flag whether we are inside a maintenance window or not
     *
     * @return  Newly built Secret
     */
    public static Secret buildSecret(Reconciliation reconciliation, ClusterCa clusterCa, Secret secret, String namespace, String secretName,
                                     String commonName, String keyCertName, Labels labels, OwnerReference ownerReference, boolean isMaintenanceTimeWindowsSatisfied) {
        Map<String, String> data = new HashMap<>(4);
        CertAndKey certAndKey = null;
        boolean shouldBeRegenerated = false;
        List<String> reasons = new ArrayList<>(2);

        if (secret == null) {
            reasons.add("certificate doesn't exist yet");
            shouldBeRegenerated = true;
        } else {
            if (clusterCa.keyCreated() || clusterCa.certRenewed() ||
                    (isMaintenanceTimeWindowsSatisfied && clusterCa.isExpiring(secret, keyCertName + ".crt")) ||
                    clusterCa.hasCaCertGenerationChanged(secret)) {
                reasons.add("certificate needs to be renewed");
                shouldBeRegenerated = true;
            }
        }

        if (shouldBeRegenerated) {
            LOGGER.debugCr(reconciliation, "Certificate for pod {} need to be regenerated because: {}", keyCertName, String.join(", ", reasons));

            try {
                certAndKey = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            }

            LOGGER.debugCr(reconciliation, "End generating certificates");
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
                    LOGGER.errorCr(reconciliation, "Error generating the keystore for {}", keyCertName, e);
                }
            }
        }

        if (certAndKey != null) {
            data.put(keyCertName + ".key", certAndKey.keyAsBase64String());
            data.put(keyCertName + ".crt", certAndKey.certAsBase64String());
            data.put(keyCertName + ".p12", certAndKey.keyStoreAsBase64String());
            data.put(keyCertName + ".password", certAndKey.storePasswordAsBase64String());
        }

        return createSecret(secretName, namespace, labels, ownerReference, data,
                Collections.singletonMap(clusterCa.caCertGenerationAnnotation(), String.valueOf(clusterCa.certGeneration())), emptyMap());
    }

    /**
     * Creates Secret
     *
     * @param name                  Name of the Secret
     * @param namespace             Namespace of the Secret
     * @param labels                Labels
     * @param ownerReference        Owner reference
     * @param data                  Data which should be stored in the Secret
     * @param customAnnotations     Custom annotations
     * @param customLabels          Custom Labels
     *
     * @return  Created Kubernetes Secret
     */
    public static Secret createSecret(String name, String namespace, Labels labels, OwnerReference ownerReference,
                                      Map<String, String> data, Map<String, String> customAnnotations, Map<String, String> customLabels) {
        if (ownerReference == null) {
            return new SecretBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(Util.mergeLabelsOrAnnotations(labels.toMap(), customLabels))
                        .withAnnotations(customAnnotations)
                    .endMetadata()
                    .withType("Opaque")
                    .withData(data)
                    .build();
        } else {
            return new SecretBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withOwnerReferences(ownerReference)
                        .withNamespace(namespace)
                        .withLabels(Util.mergeLabelsOrAnnotations(labels.toMap(), customLabels))
                        .withAnnotations(customAnnotations)
                    .endMetadata()
                    .withType("Opaque")
                    .withData(data)
                    .build();
        }
    }

    /**
     * Decodes Storage configuration from JSON into Java object
     *
     * @param json  Storage configuration in the JSON format
     *
     * @return  Storage configuration
     */
    public static Storage decodeStorageFromJson(String json) {
        try {
            return new ObjectMapper().readValue(json, Storage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encodes storage configuration from Java object to JSON
     *
     * @param storage   Storage configuration
     *
     * @return  JSON String with the configuration
     */
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

        if (currentData == null) {
            return true;
        } else {
            for (Map.Entry<String, String> entry : currentData.entrySet()) {
                String desiredValue = desiredData.get(entry.getKey());
                if (entry.getValue() != null
                        && desiredValue != null
                        && !entry.getValue().equals(desiredValue)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Checks for the list passed to this method if it is null or not. And either returns the same list, or empty list
     * if it is null.
     *
     * @param list  List which should be checked for null
     *
     * @param <T>   Type of the obejcts in the list
     *
     * @return  The original list of empty list if it as null.
     */
    public static <T> List<T> asListOrEmptyList(List<T> list) {
        return Optional.ofNullable(list)
                .orElse(Collections.emptyList());
    }

    private static String getJavaSystemPropertiesToString(List<SystemProperty> javaSystemProperties) {
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
     * Get the set of JVM options, bringing the Java system properties as well, and fill corresponding Strimzi environment variables
     * in order to pass them to the running application on the command line
     *
     * @param envVars environment variables list to put the JVM options and Java system properties
     * @param jvmOptions JVM options
     */
    public static void javaOptions(List<EnvVar> envVars, JvmOptions jvmOptions) {
        StringBuilder strimziJavaOpts = new StringBuilder();
        String xms = jvmOptions != null ? jvmOptions.getXms() : null;
        if (xms != null) {
            strimziJavaOpts.append("-Xms").append(xms);
        }

        String xmx = jvmOptions != null ? jvmOptions.getXmx() : null;
        if (xmx != null) {
            strimziJavaOpts.append(" -Xmx").append(xmx);
        }

        Map<String, String> xx = jvmOptions != null ? jvmOptions.getXx() : null;
        if (xx != null) {
            xx.forEach((k, v) -> {
                strimziJavaOpts.append(' ').append("-XX:");

                if ("true".equalsIgnoreCase(v))   {
                    strimziJavaOpts.append("+").append(k);
                } else if ("false".equalsIgnoreCase(v)) {
                    strimziJavaOpts.append("-").append(k);
                } else  {
                    strimziJavaOpts.append(k).append("=").append(v);
                }
            });
        }

        String optsTrim = strimziJavaOpts.toString().trim();
        if (!optsTrim.isEmpty()) {
            envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS, optsTrim));
        }

        List<SystemProperty> javaSystemProperties = jvmOptions != null ? jvmOptions.getJavaSystemProperties() : null;
        if (javaSystemProperties != null) {
            String propsTrim = ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties).trim();
            if (!propsTrim.isEmpty()) {
                envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, propsTrim));
            }
        }
    }

    /**
     * Adds the STRIMZI_JAVA_SYSTEM_PROPERTIES variable to the EnvVar list if any system properties were specified
     * through the provided JVM options
     *
     * @param envVars list of the Environment Variables to add to
     * @param jvmOptions JVM options
     */
    public static void jvmSystemProperties(List<EnvVar> envVars, JvmOptions jvmOptions) {
        if (jvmOptions != null) {
            String jvmSystemPropertiesString = ModelUtils.getJavaSystemPropertiesToString(jvmOptions.getJavaSystemProperties());
            if (jvmSystemPropertiesString != null && !jvmSystemPropertiesString.isEmpty()) {
                envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, jvmSystemPropertiesString));
            }
        }
    }

    /**
     * Adds the KAFKA_JVM_PERFORMANCE_OPTS variable to the EnvVar list if any performance related options were specified
     * through the provided JVM options
     *
     * @param envVars list of the Environment Variables to add to
     * @param jvmOptions JVM options
     */
    public static void jvmPerformanceOptions(List<EnvVar> envVars, JvmOptions jvmOptions) {
        StringBuilder jvmPerformanceOpts = new StringBuilder();

        Map<String, String> xx = jvmOptions != null ? jvmOptions.getXx() : null;
        if (xx != null) {
            xx.forEach((k, v) -> {
                jvmPerformanceOpts.append(' ').append("-XX:");

                if ("true".equalsIgnoreCase(v))   {
                    jvmPerformanceOpts.append("+").append(k);
                } else if ("false".equalsIgnoreCase(v)) {
                    jvmPerformanceOpts.append("-").append(k);
                } else  {
                    jvmPerformanceOpts.append(k).append("=").append(v);
                }
            });
        }

        String jvmPerformanceOptsString = jvmPerformanceOpts.toString().trim();
        if (!jvmPerformanceOptsString.isEmpty()) {
            envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS, jvmPerformanceOptsString));
        }
    }

    /**
     * Adds KAFKA_HEAP_OPTS variable to the EnvVar list if any heap related options were specified through the provided JVM options
     * If Xmx Java Options are not set STRIMZI_DYNAMIC_HEAP_PERCENTAGE and STRIMZI_DYNAMIC_HEAP_MAX may also be set by using the ResourceRequirements
     *
     * @param envVars list of the Environment Variables to add to
     * @param dynamicHeapPercentage value to set for the STRIMZI_DYNAMIC_HEAP_PERCENTAGE
     * @param dynamicHeapMaxBytes value to set for the STRIMZI_DYNAMIC_HEAP_MAX
     * @param jvmOptions JVM options
     * @param resources the resource requirements
     */
    public static void heapOptions(List<EnvVar> envVars, int dynamicHeapPercentage, long dynamicHeapMaxBytes, JvmOptions jvmOptions, ResourceRequirements resources) {
        if (dynamicHeapPercentage <= 0 || dynamicHeapPercentage > 100)  {
            throw new RuntimeException("The Heap percentage " + dynamicHeapPercentage + " is invalid. It has to be >0 and <= 100.");
        }

        StringBuilder kafkaHeapOpts = new StringBuilder();

        String xms = jvmOptions != null ? jvmOptions.getXms() : null;
        if (xms != null) {
            kafkaHeapOpts.append("-Xms")
                    .append(xms);
        }

        String xmx = jvmOptions != null ? jvmOptions.getXmx() : null;
        if (xmx != null) {
            // Honour user provided explicit max heap
            kafkaHeapOpts.append(' ').append("-Xmx").append(xmx);
        } else {
            // Get the resources => if requests are set, take request. If requests are not set, try limits
            Quantity configuredMemory = null;
            if (resources != null)  {
                if (resources.getRequests() != null && resources.getRequests().get("memory") != null)    {
                    configuredMemory = resources.getRequests().get("memory");
                } else if (resources.getLimits() != null && resources.getLimits().get("memory") != null)   {
                    configuredMemory = resources.getLimits().get("memory");
                }
            }

            if (configuredMemory != null) {
                // Delegate to the container to figure out only when CGroup memory limits are defined to prevent allocating
                // too much memory on the kubelet.

                envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE, Integer.toString(dynamicHeapPercentage)));

                if (dynamicHeapMaxBytes > 0) {
                    envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX, Long.toString(dynamicHeapMaxBytes)));
                }
            } else if (xms == null) {
                // When no memory limit, `Xms`, and `Xmx` are defined then set a default `Xms` and
                // leave `Xmx` undefined.
                kafkaHeapOpts.append("-Xms").append(AbstractModel.DEFAULT_JVM_XMS);
            }
        }

        String kafkaHeapOptsString = kafkaHeapOpts.toString().trim();
        if (!kafkaHeapOptsString.isEmpty()) {
            envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS, kafkaHeapOptsString));
        }
    }

    /**
     * Adds user-configured affinity to the AffinityBuilder
     *
     * @param builder the builder which is used to populate the node affinity
     * @param userAffinity the userAffinity which is defined by the user
     * @param topologyKey  the topology key which is used to select the node
     * @return the AffinityBuilder which has the node selector with topology key which is needed to make sure
     * the pods are scheduled only on nodes with the rack label
     */
    public static AffinityBuilder populateAffinityBuilderWithRackLabelSelector(AffinityBuilder builder, Affinity userAffinity, String topologyKey) {
        // We need to add node affinity to make sure the pods are scheduled only on nodes with the rack label
        NodeSelectorRequirement selector = new NodeSelectorRequirementBuilder()
                .withOperator("Exists")
                .withKey(topologyKey)
                .build();

        if (userAffinity != null
                && userAffinity.getNodeAffinity() != null
                && userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution() != null
                && userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms() != null) {
            // User has specified some Node Selector Terms => we should enhance them
            List<NodeSelectorTerm> oldTerms = userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms();
            List<NodeSelectorTerm> enhancedTerms = new ArrayList<>(oldTerms.size());

            for (NodeSelectorTerm term : oldTerms) {
                NodeSelectorTerm enhancedTerm = new NodeSelectorTermBuilder(term)
                        .addToMatchExpressions(selector)
                        .build();
                enhancedTerms.add(enhancedTerm);
            }

            builder = builder
                    .editOrNewNodeAffinity()
                        .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .withNodeSelectorTerms(enhancedTerms)
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity();
        } else {
            // User has not specified any selector terms => we add our own
            builder = builder
                    .editOrNewNodeAffinity()
                        .editOrNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .addNewNodeSelectorTerm()
                                .withMatchExpressions(selector)
                            .endNodeSelectorTerm()
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity();
        }
        return builder;
    }

    /**
     * Checks if the section of the custom resource has any metrics configuration and sets it in the AbstractModel.
     *
     * @param model                     The cluster model where the metrics will be configured
     * @param resourceWithMetrics       The section of the resource with metrics configuration
     */
    public static void parseMetrics(AbstractModel model, HasConfigurableMetrics resourceWithMetrics)   {
        if (resourceWithMetrics.getMetricsConfig() != null)    {
            model.isMetricsEnabled = true;
            model.setMetricsConfigInCm(resourceWithMetrics.getMetricsConfig());
        }
    }

    /**
     * Creates the OwnerReference based on the resource passed as parameter
     *
     * @param owner         The resource which should be the owner
     * @param isController  Indicates whether the owner acts also as the controller. This value is used in the
     *                      controller flag which is part of the OwnerReference object.
     *
     * @return          The new OwnerReference
     */
    public static OwnerReference createOwnerReference(HasMetadata owner, boolean isController)   {
        return new OwnerReferenceBuilder()
                .withApiVersion(owner.getApiVersion())
                .withKind(owner.getKind())
                .withName(owner.getMetadata().getName())
                .withUid(owner.getMetadata().getUid())
                .withBlockOwnerDeletion(false)
                .withController(isController)
                .build();
    }

    /**
     * Checks whether the resource has given Owner Reference among its list of owner references
     *
     * @param resource  Resource which should be checked for OwnerReference
     * @param owner     OwnerReference which should be verified
     *
     * @return          True if the owner reference is found. False otherwise.
     */
    public static boolean hasOwnerReference(HasMetadata resource, OwnerReference owner)    {
        if (resource.getMetadata().getOwnerReferences() != null) {
            return resource.getMetadata().getOwnerReferences()
                    .stream()
                    .anyMatch(o -> owner.getApiVersion().equals(o.getApiVersion())
                            && owner.getKind().equals(o.getKind())
                            && owner.getName().equals(o.getName())
                            && owner.getUid().equals(o.getUid()));
        } else {
            return false;
        }
    }

    /**
     * Extracts the CA generation from the CA
     *
     * @param ca    CA from which the generation should be extracted
     *
     * @return      CA generation or the initial generation if no generation is set
     */
    public static int caCertGeneration(Ca ca) {
        return Annotations.intAnnotation(ca.caCertSecret(), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, Ca.INIT_GENERATION);
    }

    /**
     * Returns the id of a pod given the pod name
     *
     * @param podName   Name of pod
     *
     * @return          Id of the pod
     */
    public static int idOfPod(String podName)  {
        return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
    }

    /**
     * Generates all possible DNS names for a Kubernetes service:
     *     - service-name
     *     - service-name.namespace
     *     - service-name.namespace.svc
     *     - service-name.namespace.svc.dns.suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     *
     * @return  List with all possible DNS names
     */
    public static List<String> generateAllServiceDnsNames(String namespace, String serviceName)    {
        DnsNameGenerator kafkaDnsGenerator = DnsNameGenerator.of(namespace, serviceName);

        List<String> dnsNames = new ArrayList<>(4);

        dnsNames.add(serviceName);
        dnsNames.add(String.format("%s.%s", serviceName, namespace));
        dnsNames.add(kafkaDnsGenerator.serviceDnsNameWithoutClusterDomain());
        dnsNames.add(kafkaDnsGenerator.serviceDnsName());

        return dnsNames;
    }

    /**
     * Validate cpu and memory resources.
     * Early resources validation avoids triggering any pod operation with invalid configuration.
     * 
     * For both cpu and memory, this method checks that request is greater than zero, 
     * limit is greater than zero and limit is greater than or equal to the request.
     * 
     * @param resources Resources configuration.
     * @param path Resources path.
     */
    public static void validateComputeResources(ResourceRequirements resources, String path) {
        List<String> errors = ModelUtils.validateComputeResources(resources, "cpu", path);
        errors.addAll(ModelUtils.validateComputeResources(resources, "memory", path));
        if (errors.size() > 0) {
            throw new InvalidResourceException(errors.toString());
        }
    }

    /**
     * Validate compute resources.
     * 
     * This method checks that request is greater than zero, limit is greater than zero 
     * and limit is greater than or equal to the request.
     * 
     * @param resources Resources configuration.
     * @param type Resources type.
     * @param path Resources path.
     * 
     * @return Error set.
     */
    private static List<String> validateComputeResources(ResourceRequirements resources, String type, String path) {
        List<String> errors = new ArrayList<>();
        if (resources != null) {
            if (resources.getRequests() != null && resources.getRequests().get(type) != null) {
                Quantity request = resources.getRequests().get(type);
                if (request != null && request.getNumericalAmount().compareTo(BigDecimal.ZERO) <= 0) {
                    errors.add(String.format("%s %s request must be > zero", path, type).trim());
                }
            }
            if (resources.getLimits() != null && resources.getLimits().get(type) != null) {
                Quantity limit = resources.getLimits().get(type);
                if (limit.getNumericalAmount().compareTo(BigDecimal.ZERO) <= 0) {
                    errors.add(String.format("%s %s limit must be > zero", path, type).trim());
                }
            }
            if (resources.getRequests() != null && resources.getRequests().get(type) != null
                    && resources.getLimits() != null && resources.getLimits().get(type) != null) {
                Quantity limit = resources.getLimits().get(type);
                Quantity request = resources.getRequests().get(type);
                if (request != null && request.getNumericalAmount().compareTo(limit.getNumericalAmount()) > 0) {
                    errors.add(String.format("%s %s request must be <= limit", path, type).trim());
                }
            }
        }
        return errors;
    }
}
