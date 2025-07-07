/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
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
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ModelUtils is a utility class that holds generic static helper functions
 * These are generally to be used within the classes that extend the AbstractModel class
 */
public class ModelUtils {

    private ModelUtils() {}

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ModelUtils.class.getName());

    /**
     * Extract certificate validity days from cluster CA configuration
     *
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
     * Extract certificate renewal days from cluster CA configuration
     *
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

    /**
     * This method transforms a String into a List of Strings, where each entry is an uncommented line of input.
     * The lines beginning with '#' (comments) are ignored.
     * @param config ConfigMap data as a String
     * @return List of String key=value
     */
    public static List<String> getLinesWithoutCommentsAndEmptyLines(String config) {
        List<String> validLines = new ArrayList<>();
        if (config != null) {
            for (String line : config.split("\\r?\\n")) {
                if (!line.isEmpty() && !line.matches("\\s*\\#.*")) {
                    validLines.add(line);
                }
            }
        }
        return validLines;
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
        if (!errors.isEmpty()) {
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
