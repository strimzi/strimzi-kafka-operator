/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.admissionregistration.v1.ValidatingWebhookConfiguration;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.draincleaner.DrainCleanerResource;
import io.strimzi.systemtest.resources.kubernetes.ClusterOperatorCustomResourceDefinition;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleResource;
import io.strimzi.systemtest.resources.kubernetes.ConfigMapResource;
import io.strimzi.systemtest.resources.kubernetes.DeploymentResource;
import io.strimzi.systemtest.resources.kubernetes.JobResource;
import io.strimzi.systemtest.resources.kubernetes.LeaseResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.RoleResource;
import io.strimzi.systemtest.resources.kubernetes.SecretResource;
import io.strimzi.systemtest.resources.kubernetes.ServiceAccountResource;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.resources.kubernetes.ValidatingWebhookConfigurationResource;
import io.strimzi.systemtest.resources.openshift.OperatorGroupResource;
import io.strimzi.systemtest.resources.openshift.SubscriptionResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity",
    "checkstyle:ClassDataAbstractionCoupling"})
@SuppressFBWarnings({"MS_MUTABLE_COLLECTION_PKGPROTECT", "MS_MUTABLE_COLLECTION"})
public class ResourceManager {

    private static final Logger LOGGER = LogManager.getLogger(ResourceManager.class);

    public static final long CR_CREATION_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness();

    public static final Map<String, Stack<ResourceItem>> STORED_RESOURCES = new LinkedHashMap<>();

    private static String coDeploymentName = TestConstants.STRIMZI_DEPLOYMENT_NAME;

    private static ThreadLocal<ExtensionContext> testContext = new ThreadLocal<>();
    private static ResourceManager instance;

    public static synchronized ResourceManager getInstance() {
        if (instance == null) {
            instance = new ResourceManager();
        }
        return instance;
    }

    public static KubeClient kubeClient() {
        return KubeClusterResource.kubeClient();
    }

    public static KubeCmdClient cmdKubeClient() {
        return KubeClusterResource.cmdKubeClient();
    }

    public static HelmClient helmClient() {
        return KubeClusterResource.helmClusterClient();
    }

    public static void setTestContext(ExtensionContext context) {
        testContext.set(context);
    }

    public static ExtensionContext getTestContext() {
        return testContext.get();
    }

    private final ResourceType<?>[] resourceTypes = new ResourceType[]{
        new KafkaBridgeResource(),
        new KafkaClientsResource(),
        new KafkaConnectorResource(),
        new KafkaConnectResource(),
        new KafkaMirrorMaker2Resource(),
        new KafkaMirrorMakerResource(),
        new KafkaRebalanceResource(),
        new KafkaResource(),
        new KafkaTopicResource(),
        new KafkaUserResource(),
        new BundleResource(),
        new ClusterRoleBindingResource(),
        new DeploymentResource(),
        new JobResource(),
        new NetworkPolicyResource(),
        new RoleBindingResource(),
        new ServiceResource(),
        new ConfigMapResource(),
        new LeaseResource(),
        new ServiceAccountResource(),
        new RoleResource(),
        new ClusterRoleResource(),
        new ClusterOperatorCustomResourceDefinition(),
        new SecretResource(),
        new ValidatingWebhookConfigurationResource(),
        new SubscriptionResource(),
        new OperatorGroupResource(),
        new KafkaNodePoolResource()
    };
    @SafeVarargs
    public final <T extends HasMetadata> void createResourceWithoutWait(T... resources) {
        createResource(false, resources);
    }

    @SafeVarargs
    public final <T extends HasMetadata> void createResourceWithWait(T... resources) {
        createResource(true, resources);
    }

    @SafeVarargs
    private <T extends HasMetadata> void createResource(boolean waitReady, T... resources) {
        for (T resource : resources) {
            ResourceType<T> type = findResourceType(resource);

            setNamespaceInResource(resource);

            if (resource.getMetadata().getNamespace() == null) {
                LOGGER.info("Creating/Updating {} {}",
                        resource.getKind(), resource.getMetadata().getName());
            } else {
                LOGGER.info("Creating/Updating {} {}/{}",
                        resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            }

            labelResource(resource);

            // adding test.suite and test.case labels to the PodTemplate
            if (resource.getKind().equals(TestConstants.JOB)) {
                this.copyTestSuiteAndTestCaseControllerLabelsIntoPodTemplate(resource, ((Job) resource).getSpec().getTemplate());
            } else if (resource.getKind().equals(TestConstants.DEPLOYMENT)) {
                this.copyTestSuiteAndTestCaseControllerLabelsIntoPodTemplate(resource, ((Deployment) resource).getSpec().getTemplate());
            }
            type.create(resource);

            synchronized (this) {
                STORED_RESOURCES.computeIfAbsent(getTestContext().getDisplayName(), k -> new Stack<>());
                STORED_RESOURCES.get(getTestContext().getDisplayName()).push(
                    new ResourceItem<T>(
                        () -> deleteResource(resource),
                        resource
                    ));
            }
        }

        if (waitReady) {
            for (T resource : resources) {
                ResourceType<T> type = findResourceType(resource);
                if (Objects.equals(resource.getKind(), KafkaTopic.RESOURCE_KIND)) {
                    continue;
                }
                assertTrue(waitResourceCondition(resource, ResourceCondition.readiness(type)),
                    String.format("Timed out waiting for %s %s/%s to be ready", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()));
            }
        }
    }

    private <T extends HasMetadata> void labelResource(T resource) {
        // If we are create resource in test case we annotate it with label. This is needed for filtering when
        // we collect logs from Pods, ReplicaSets, Deployments etc.
        Map<String, String> labels;
        if (getTestContext().getTestMethod().isPresent()) {
            String testCaseName = getTestContext().getRequiredTestMethod().getName();
            // because label values `must be no more than 63 characters`
            if (testCaseName.length() > 63) {
                // we cut to 62 characters
                testCaseName = testCaseName.substring(0, 62);
            }

            if (resource.getMetadata().getLabels() == null) {
                labels = new HashMap<>();
                labels.put(TestConstants.TEST_CASE_NAME_LABEL, testCaseName);
            } else {
                labels = new HashMap<>(resource.getMetadata().getLabels());
                labels.put(TestConstants.TEST_CASE_NAME_LABEL, testCaseName);
            }
            resource.getMetadata().setLabels(labels);
        } else {
            // this is labeling for shared resources in @BeforeAll
            if (getTestContext().getTestClass().isPresent()) {
                final String testSuiteName = StUtils.removePackageName(getTestContext().getRequiredTestClass().getName());

                if (resource.getMetadata().getLabels() == null) {
                    labels = new HashMap<>();
                    labels.put(TestConstants.TEST_SUITE_NAME_LABEL, testSuiteName);
                } else {
                    labels = new HashMap<>(resource.getMetadata().getLabels());
                    labels.put(TestConstants.TEST_SUITE_NAME_LABEL, testSuiteName);
                }
                resource.getMetadata().setLabels(labels);
            }
        }
    }

    private <T extends HasMetadata> void setNamespaceInResource(T resource) {
        // if it is parallel namespace test we are gonna replace resource a namespace
        if (StUtils.isParallelNamespaceTest(getTestContext())) {
            if (!Environment.isNamespaceRbacScope()) {
                final String namespace = getTestContext().getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.NAMESPACE_KEY).toString();
                LOGGER.info("Setting Namespace: {} to resource: {}/{}", namespace, resource.getKind(), resource.getMetadata().getName());
                resource.getMetadata().setNamespace(namespace);
            }
        }
    }

    @SafeVarargs
    public final <T extends HasMetadata> void deleteResource(T... resources) {
        for (T resource : resources) {
            ResourceType<T> type = findResourceType(resource);
            if (type == null) {
                LOGGER.warn("Can't find resource type, please delete it manually");
                continue;
            }

            if (resource.getMetadata().getNamespace() == null) {
                LOGGER.info("Deleting of {} {}",
                        resource.getKind(), resource.getMetadata().getName());
            } else {
                LOGGER.info("Deleting of {} {}/{}",
                        resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            }

            try {
                type.delete(resource);
                assertTrue(waitResourceCondition(resource, ResourceCondition.deletion()),
                        String.format("Timed out deleting %s %s/%s", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()));
            } catch (Exception e)   {
                if (resource.getMetadata().getNamespace() == null) {
                    LOGGER.error("Failed to delete {} {}", resource.getKind(), resource.getMetadata().getName(), e);
                } else {
                    LOGGER.error("Failed to delete {} {}/{}", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), e);
                }
            }

        }
    }

    @SafeVarargs
    public final <T extends HasMetadata> void updateResource(T... resources) {
        for (T resource : resources) {
            ResourceType<T> type = findResourceType(resource);
            type.update(resource);
        }
    }

    public final <T extends HasMetadata> boolean waitResourceCondition(T resource, ResourceCondition<T> condition) {
        assertNotNull(resource);
        assertNotNull(resource.getMetadata());
        assertNotNull(resource.getMetadata().getName());

        // cluster role binding and custom resource definition does not need namespace...
        if (!(resource instanceof ClusterRoleBinding || resource instanceof CustomResourceDefinition || resource instanceof ClusterRole || resource instanceof ValidatingWebhookConfiguration)) {
            assertNotNull(resource.getMetadata().getNamespace());
        }

        ResourceType<T> type = findResourceType(resource);
        assertNotNull(type);
        boolean[] resourceReady = new boolean[1];

        TestUtils.waitFor("resource condition: " + condition.getConditionName() + " to be fulfilled for resource " + resource.getKind() + ":" + resource.getMetadata().getName(),
            TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, ResourceOperation.getTimeoutForResourceReadiness(resource.getKind()),
            () -> {
                T res = type.get(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
                resourceReady[0] =  condition.getPredicate().test(res);
                if (!resourceReady[0]) {
                    type.delete(res);
                }
                return resourceReady[0];
            });

        return resourceReady[0];
    }

    /**
     * Auxiliary method for copying {@link TestConstants#TEST_SUITE_NAME_LABEL} and {@link TestConstants#TEST_CASE_NAME_LABEL} labels
     * into PodTemplate ensuring that in case of failure {@link io.strimzi.systemtest.logs.LogCollector} will collect all
     * related Pods, which corespondents to such Controller (i.e., Job, Deployment)
     *
     * @param resource controller resource from which we copy test suite or test case labels
     * @param resourcePodTemplate {@link PodTemplateSpec} of the specific resource
     * @param <T> resource, which sings contract with {@link HasMetadata} interface
     * @param <R> {@link PodTemplateSpec}
     */
    private final <T extends HasMetadata, R extends PodTemplateSpec> void copyTestSuiteAndTestCaseControllerLabelsIntoPodTemplate(final T resource, final R resourcePodTemplate) {
        if (resource.getMetadata().getLabels() != null && resourcePodTemplate.getMetadata().getLabels() != null) {
            // 1. fetch Controller and PodTemplate labels
            final Map<String, String> controllerLabels = new HashMap<>(resource.getMetadata().getLabels());
            final Map<String, String> podLabels = new HashMap<>(resourcePodTemplate.getMetadata().getLabels());

            // 2. a) add label for test.suite
            if (controllerLabels.containsKey(TestConstants.TEST_SUITE_NAME_LABEL)) {
                podLabels.putIfAbsent(TestConstants.TEST_SUITE_NAME_LABEL, controllerLabels.get(TestConstants.TEST_SUITE_NAME_LABEL));
            }
            // 2. b) add label for test.case
            if (controllerLabels.containsKey(TestConstants.TEST_CASE_NAME_LABEL)) {
                podLabels.putIfAbsent(TestConstants.TEST_CASE_NAME_LABEL, controllerLabels.get(TestConstants.TEST_CASE_NAME_LABEL));
            }
            // 3. modify PodTemplates labels for LogCollector using reference and thus not need to return
            resourcePodTemplate.getMetadata().setLabels(podLabels);
        }
    }

    /**
     * Synchronizing all resources which are inside specific extension context.
     * @param <T> type of the resource which inherits from HasMetadata f.e Kafka, KafkaConnect, Pod, Deployment etc..
     */
    @SuppressWarnings(value = "unchecked")
    public final <T extends HasMetadata> void synchronizeResources() {
        Stack<ResourceItem> resources = STORED_RESOURCES.get(getTestContext().getDisplayName());

        // sync all resources
        for (ResourceItem resource : resources) {
            if (resource.getResource() == null) {
                continue;
            }
            ResourceType<T> type = findResourceType((T) resource.getResource());

            waitResourceCondition((T) resource.getResource(), ResourceCondition.readiness(type));
        }
    }

    public static <T extends CustomResource, L extends DefaultKubernetesResourceList<T>> void replaceCrdResource(Class<T> crdClass, Class<L> listClass, String resourceName, Consumer<T> editor, String namespace) {
        T toBeReplaced = Crds.operation(kubeClient().getClient(), crdClass, listClass).inNamespace(namespace).withName(resourceName).get();
        editor.accept(toBeReplaced);
        Crds.operation(kubeClient().getClient(), crdClass, listClass).inNamespace(namespace).resource(toBeReplaced).update();
    }

    public void deleteResources() {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        if (!STORED_RESOURCES.containsKey(getTestContext().getDisplayName()) || STORED_RESOURCES.get(getTestContext().getDisplayName()).isEmpty()) {
            LOGGER.info("In context {} is everything deleted", getTestContext().getDisplayName());
        } else {
            LOGGER.info("Deleting all resources for {}", getTestContext().getDisplayName());
        }

        // if stack is created for specific test suite or test case
        AtomicInteger numberOfResources = STORED_RESOURCES.get(getTestContext().getDisplayName()) != null ?
            new AtomicInteger(STORED_RESOURCES.get(getTestContext().getDisplayName()).size()) :
            // stack has no elements
            new AtomicInteger(0);
        while (STORED_RESOURCES.containsKey(getTestContext().getDisplayName()) && numberOfResources.get() > 0) {
            Stack<ResourceItem> s = STORED_RESOURCES.get(getTestContext().getDisplayName());

            while (!s.isEmpty()) {
                ResourceItem resourceItem = s.pop();

                try {
                    resourceItem.getThrowableRunner().run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                numberOfResources.decrementAndGet();
            }
        }
        STORED_RESOURCES.remove(getTestContext().getDisplayName());
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
    }

    /**
     * Log actual status of custom resource with Pods.
     * @param customResource - Kafka, KafkaConnect etc. - every resource that HasMetadata and HasStatus (Strimzi status)
     */
    public static <T extends CustomResource<? extends Spec, ? extends Status>> void logCurrentResourceStatus(T customResource) {
        if (customResource != null) {
            List<String> printWholeCR = Arrays.asList(KafkaConnector.RESOURCE_KIND, KafkaTopic.RESOURCE_KIND, KafkaUser.RESOURCE_KIND);

            String kind = customResource.getKind();
            String name = customResource.getMetadata().getName();

            if (printWholeCR.contains(kind)) {
                LOGGER.info(customResource);
            } else {
                List<String> log = new ArrayList<>(asList(kind, " status:\n", "\nConditions:\n"));

                if (customResource.getStatus() != null) {
                    List<Condition> conditions = customResource.getStatus().getConditions();
                    if (conditions != null) {
                        for (Condition condition : customResource.getStatus().getConditions()) {
                            if (condition.getMessage() != null) {
                                log.add("\tType: " + condition.getType() + "\n");
                                log.add("\tMessage: " + condition.getMessage() + "\n");
                            }
                        }
                    }

                    log.add("\nPods with conditions and messages:\n\n");

                    for (Pod pod : kubeClient().namespace(customResource.getMetadata().getNamespace()).listPodsByPrefixInName(name)) {
                        log.add(pod.getMetadata().getName() + ":");
                        for (PodCondition podCondition : pod.getStatus().getConditions()) {
                            if (podCondition.getMessage() != null) {
                                log.add("\n\tType: " + podCondition.getType() + "\n");
                                log.add("\tMessage: " + podCondition.getMessage() + "\n");
                            } else {
                                log.add("\n\tType: <EMPTY>\n");
                                log.add("\tMessage: <EMPTY>\n");
                            }
                        }
                        log.add("\n\n");
                    }
                    LOGGER.info("{}", String.join("", log).strip());
                }
            }
        }
    }

    /**
     * Wait until the CR is in desired state
     * @param operation - client of CR - for example kafkaClient()
     * @param resource - custom resource
     * @param statusType - desired status
     * @return returns CR
     */
    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> statusType, long resourceTimeout) {
        return waitForResourceStatus(operation, resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), statusType, ConditionStatus.True, resourceTimeout);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> statusType, ConditionStatus conditionStatus, long resourceTimeout) {
        return waitForResourceStatus(operation, resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), statusType, conditionStatus, resourceTimeout);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, String kind, String namespace, String name, Enum<?> statusType, long resourceTimeoutMs) {
        return waitForResourceStatus(operation, kind, namespace, name, statusType, ConditionStatus.True, resourceTimeoutMs);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, String kind, String namespace, String name, Enum<?> statusType, ConditionStatus conditionStatus, long resourceTimeoutMs) {
        LOGGER.info("Waiting for {}: {}/{} will have desired state: {}", kind, namespace, name, statusType);

        TestUtils.waitFor(String.format("%s: %s#%s will have desired state: %s", kind, namespace, name, statusType),
            TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, resourceTimeoutMs,
            () -> operation.inNamespace(namespace)
                .withName(name)
                .get().getStatus().getConditions().stream().anyMatch(condition -> condition.getType().equals(statusType.toString()) && condition.getStatus().equals(conditionStatus.toString())),
            () -> logCurrentResourceStatus(operation.inNamespace(namespace)
                .withName(name)
                .get()));

        LOGGER.info("{}: {}/{} is in desired state: {}", kind, namespace, name, statusType);
        return true;
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> status) {
        long resourceTimeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());
        return waitForResourceStatus(operation, resource, status, resourceTimeout);
    }

    public static String getCoDeploymentName() {
        return coDeploymentName;
    }

    public static void setCoDeploymentName(String newName) {
        coDeploymentName = newName;
    }

    public static void waitForResourceReadiness(String resourceType, String resourceName) {
        LOGGER.info("Waiting for " + resourceType + "/" + resourceName + " readiness");

        TestUtils.waitFor("readiness of resource " + resourceType + "/" + resourceName,
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_CMD_CLIENT_TIMEOUT,
            () -> ResourceManager.cmdKubeClient().getResourceReadiness(resourceType, resourceName));
        LOGGER.info("Resource " + resourceType + "/" + resourceName + " is ready");
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatusMessage(MixedOperation<T, ?, ?> operation, T resource, String message) {
        long resourceTimeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());
        return waitForResourceStatusMessage(operation, resource, message, resourceTimeout);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatusMessage(MixedOperation<T, ?, ?> operation, T resource, String message, long resourceTimeout) {
        waitForResourceStatusMessage(operation, resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), message, resourceTimeout);
        return true;
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatusMessage(MixedOperation<T, ?, ?> operation, String kind, String namespace, String name, String message, long resourceTimeoutMs) {
        LOGGER.info("Waiting for {}: {}/{} will contain desired status message: {}", kind, namespace, name, message);

        TestUtils.waitFor(String.format("%s: %s#%s will contain desired status message: %s", kind, namespace, name, message),
            TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, resourceTimeoutMs,
            () -> operation.inNamespace(namespace)
                .withName(name)
                .get().getStatus().getConditions().stream().anyMatch(condition -> condition.getMessage().contains(message) && condition.getStatus().equals("True")),
            () -> logCurrentResourceStatus(operation.inNamespace(namespace)
                .withName(name)
                .get()));

        LOGGER.info("{}: {}/{} contains desired message in status: {}", kind, namespace, name, message);
        return true;
    }

    @SuppressWarnings(value = "unchecked")
    private <T extends HasMetadata> ResourceType<T> findResourceType(T resource) {

        // for conflicting deployment types
        if (resource.getKind().equals(TestConstants.DEPLOYMENT)) {
            String deploymentType = resource.getMetadata().getLabels().get(TestConstants.DEPLOYMENT_TYPE);
            DeploymentTypes deploymentTypes = DeploymentTypes.valueOf(deploymentType);

            switch (deploymentTypes) {
                case BundleClusterOperator:
                    return (ResourceType<T>) new BundleResource();
                case KafkaClients:
                    return (ResourceType<T>) new KafkaClientsResource();
                case DrainCleaner:
                    return (ResourceType<T>) new DrainCleanerResource();
                default:
                    return (ResourceType<T>) new DeploymentResource();
            }
        }

        // other no conflicting types
        for (ResourceType<?> type : resourceTypes) {
            if (type.getKind().equals(resource.getKind())) {
                return (ResourceType<T>) type;
            }
        }
        return null;
    }
}
