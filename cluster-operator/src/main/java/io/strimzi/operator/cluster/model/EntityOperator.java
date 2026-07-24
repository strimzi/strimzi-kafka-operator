/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.common.StrimziProbe;
import io.strimzi.api.kafka.model.common.StrimziProbeBuilder;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.StrimziDeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.TemplateUtils.addAdditionalVolumes;

/**
 * Represents the Entity Operator deployment
 */
public class EntityOperator extends AbstractModel {
    /**
     * Type of the component which this model class represents. It is used for labeling and naming purposes.
     */
    public static final String COMPONENT_TYPE = "entity-operator";

    protected static final String CO_ENV_VAR_CUSTOM_ENTITY_OPERATOR_POD_LABELS = "STRIMZI_CUSTOM_ENTITY_OPERATOR_LABELS";

    /**
     * Represents which operator permissions should be included when filtering ClusterRole rules
     */
    public enum Permissions {
        /** Include only Topic Operator permissions (kafkatopics resources) */
        TOPIC_OPERATOR,

        /** Include only User Operator permissions (kafkausers and secrets resources) */
        USER_OPERATOR,

        /** Include both Topic and User Operator permissions (no filtering) */
        BOTH
    }

    /**
     * Map defining which resource prefixes belong to each permission type.
     * Used for filtering ClusterRole rules to enforce least-privilege permissions.
     */
    private static final Map<Permissions, List<String>> OPERATOR_RESOURCE_PREFIXES = Map.of(
        Permissions.TOPIC_OPERATOR, List.of("kafkatopics"),
        Permissions.USER_OPERATOR, List.of("kafkausers", "secrets")
    );

    /**
     * Default health check options used by the Topic and User operators
     */
    protected static final StrimziProbe DEFAULT_HEALTHCHECK_OPTIONS = new StrimziProbeBuilder().withTimeoutSeconds(5).withInitialDelaySeconds(10).build();

    private EntityTopicOperator topicOperator;
    private EntityUserOperator userOperator;
    /* test */ boolean cruiseControlEnabled;

    private ResourceTemplate templateRole;
    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;
    private PodDisruptionBudgetTemplate templatePodDisruptionBudget;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_ENTITY_OPERATOR_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Kafka custom resource
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected EntityOperator(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaResources.entityOperatorDeploymentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);
    }

    /**
     * Create an Entity Operator from a given desired resource
     *
     * @param reconciliation                The reconciliation marker
     * @param kafkaAssembly                 Desired resource with cluster configuration containing the Entity Operator one
     * @param sharedEnvironmentProvider     Shared environment provider
     * @param config                        Cluster Operator configuration
     *
     * @return Entity Operator instance, null if not configured in the ConfigMap
     */
    public static EntityOperator fromCrd(Reconciliation reconciliation,
                                         Kafka kafkaAssembly,
                                         SharedEnvironmentProvider sharedEnvironmentProvider,
                                         ClusterOperatorConfig config) {
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();

        if (entityOperatorSpec != null
                && (entityOperatorSpec.getUserOperator() != null || entityOperatorSpec.getTopicOperator() != null)) {
            EntityOperator result = new EntityOperator(reconciliation, kafkaAssembly, sharedEnvironmentProvider);

            EntityTopicOperator topicOperator = EntityTopicOperator.fromCrd(reconciliation, kafkaAssembly, sharedEnvironmentProvider, config);
            EntityUserOperator userOperator = EntityUserOperator.fromCrd(reconciliation, kafkaAssembly, sharedEnvironmentProvider, config);

            result.topicOperator = topicOperator;
            result.cruiseControlEnabled = kafkaAssembly.getSpec().getCruiseControl() != null;
            result.userOperator = userOperator;

            if (entityOperatorSpec.getTemplate() != null) {
                EntityOperatorTemplate template = entityOperatorSpec.getTemplate();

                result.templateRole = template.getEntityOperatorRole();
                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateServiceAccount = template.getServiceAccount();
                result.templatePodDisruptionBudget = template.getPodDisruptionBudget();

                if (topicOperator != null) {
                    topicOperator.templateContainer = template.getTopicOperatorContainer();
                }

                if (userOperator != null) {
                    userOperator.templateContainer = template.getUserOperatorContainer();
                }
            }

            return result;
        } else {
            return null;
        }
    }

    /**
     * Gets the Topic Operator model.
     *
     * @return  The Topic Operator model
     */
    public EntityTopicOperator topicOperator() {
        return topicOperator;
    }

    /**
     * Gets the User Operator model.
     *
     * @return  The User Operator model
     */
    public EntityUserOperator userOperator() {
        return userOperator;
    }

    /**
     * Generates the Entity Operator deployment
     *
     * @param podAnnotations    Map with the annotations that will be used for the Pod metadata
     * @param isOpenShift       Flag which identifies if we are running on OpenShift
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  Image pull secrets
     *
     * @return  Kubernetes Deployment with the Entity Operator
     */
    public Deployment generateDeployment(Map<String, String> podAnnotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        PodSecurityProviderContext podSecurityProviderContext = new PodSecurityProviderContextImpl(templatePod);

        return WorkloadUtils.createDeployment(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateDeployment,
                1,
                null,
                WorkloadUtils.deploymentStrategy(StrimziDeploymentStrategy.RECREATE), // we intentionally ignore the template here as EO doesn't support RU strategy
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        podAnnotations,
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        createContainers(imagePullPolicy),
                        getVolumes(isOpenShift),
                        imagePullSecrets,
                        securityProvider.entityOperatorPodSecurityContext(podSecurityProviderContext),
                        securityProvider.entityOperatorHostUsers(podSecurityProviderContext))
        );
    }

    /* test */ List<Container> createContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(3);

        if (topicOperator != null) {
            containers.add(topicOperator.createContainer(imagePullPolicy));
        }
        if (userOperator != null) {
            containers.add(userOperator.createContainer(imagePullPolicy));
        }

        return containers;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>();

        volumeList.add(VolumeUtils.createServiceAccountVolume());

        if (topicOperator != null) {
            volumeList.addAll(topicOperator.getVolumes(templatePod, isOpenShift));
        }

        if (userOperator != null) {
            volumeList.addAll(userOperator.getVolumes(templatePod));
        }
        
        addAdditionalVolumes(templatePod, volumeList);

        return volumeList;
    }

    /**
     * Filters PolicyRules based on permissions to provide least-privilege access.
     * Rules that reference both TO and UO resources are split to include only the
     * relevant resources for the specified permissions.
     *
     * @param rules        The original list of PolicyRules from the combined ClusterRole
     * @param permissions  Which operator permissions to include
     * @return Filtered list of PolicyRules
     */
    private List<PolicyRule> filterRulesByPermissions(List<PolicyRule> rules, Permissions permissions) {
        if (permissions == Permissions.BOTH) {
            // No filtering needed, return all rules
            return rules;
        }

        List<PolicyRule> filteredRules = new ArrayList<>();

        for (PolicyRule rule : rules) {
            // Filter resources that match the permissions
            List<String> resourcesToKeep = rule.getResources().stream()
                    .filter(resource -> matchesPermissions(resource, permissions))
                    .toList();

            // If this rule has relevant resources, create a new rule with only those resources
            if (!resourcesToKeep.isEmpty()) {
                PolicyRule filteredRule = new PolicyRuleBuilder(rule)
                        .withResources(resourcesToKeep)
                        .build();
                filteredRules.add(filteredRule);
            }
        }

        return filteredRules;
    }

    /**
     * Checks if a resource matches the specified permissions based on the OPERATOR_RESOURCE_PREFIXES map.
     * Handles both exact matches (e.g., "secrets") and subresource matches (e.g., "kafkatopics/status").
     *
     * @param resource     The resource name (e.g., "kafkatopics", "kafkausers/status", "secrets")
     * @param permissions  The permissions to match against
     * @return true if this resource belongs to the specified permissions
     */
    private boolean matchesPermissions(String resource, Permissions permissions) {
        return OPERATOR_RESOURCE_PREFIXES.get(permissions).stream()
                .anyMatch(prefix -> resource.equals(prefix) || resource.startsWith(prefix + "/"));
    }

    /**
     * Generate a Role with filtered permissions based on which operator(s) need access.
     * Loads the combined Entity Operator ClusterRole template and filters the rules
     * based on the specified permissions to enforce least-privilege access.
     *
     * @param ownerNamespace   The namespace of the parent resource (the Kafka CR)
     * @param namespace        The namespace where this role will be located
     * @param roleName         The name to use for the Role resource
     * @param permissions      Which operator permissions to include (TOPIC_OPERATOR, USER_OPERATOR, or BOTH)
     *
     * @return Role with appropriately filtered permissions
     */
    public Role generateRole(String ownerNamespace, String namespace, String roleName, Permissions permissions) {
        List<PolicyRule> rules;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                    EntityOperator.class.getResourceAsStream("/cluster-roles/031-ClusterRole-strimzi-entity-operator.yaml"),
                    StandardCharsets.UTF_8)
            )
        ) {
            String yaml = br.lines().collect(Collectors.joining(System.lineSeparator()));
            ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
            ClusterRole cr = yamlReader.readValue(yaml, ClusterRole.class);
            rules = filterRulesByPermissions(cr.getRules(), permissions);
        } catch (IOException e) {
            LOGGER.errorCr(reconciliation, "Failed to read entity-operator ClusterRole.", e);
            throw new RuntimeException(e);
        }

        Role role = RbacUtils.createRole(roleName, namespace, rules, labels, ownerReference, templateRole);

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(ownerNamespace)) {
            role.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return role;
    }

    /**
     * Generates the NetworkPolicies relevant for Entity Operator
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy() {
        // List of network policy rules for all ports
        List<NetworkPolicyIngressRule> rules = new ArrayList<>();

        // For Topic Operator
        if (topicOperator != null) {
            rules.add(NetworkPolicyUtils.createIngressRule(EntityTopicOperator.HEALTHCHECK_PORT, List.of()));
        }

        // For User Operator
        if (userOperator != null) {
            rules.add(NetworkPolicyUtils.createIngressRule(EntityUserOperator.HEALTHCHECK_PORT, List.of()));
        }

        // Build the final network policy with all rules covering all the ports
        return NetworkPolicyUtils.createNetworkPolicy(
                componentName,
                namespace,
                labels,
                ownerReference,
                rules
        );
    }
    /**
     * Generates the PodDisruptionBudget for the Entity Operator
     *
     * @return The PodDisruptionBudget for the Entity Operator
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudget(
                componentName,
                namespace,
                labels,
                ownerReference,
                templatePodDisruptionBudget,
                1
        );
    }
}
