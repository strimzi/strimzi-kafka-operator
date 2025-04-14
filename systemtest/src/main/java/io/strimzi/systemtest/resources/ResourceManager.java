/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.TestTags;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.function.Consumer;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity",
    "checkstyle:ClassDataAbstractionCoupling"})
@SuppressFBWarnings({"MS_MUTABLE_COLLECTION_PKGPROTECT", "MS_MUTABLE_COLLECTION"})
public class ResourceManager {

    private static final Logger LOGGER = LogManager.getLogger(ResourceManager.class);

    public static final Map<String, Stack<ResourceItem>> STORED_RESOURCES = new LinkedHashMap<>();

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

    public static void setTestContext(ExtensionContext context) {
        testContext.set(context);
    }

    public static ExtensionContext getTestContext() {
        return testContext.get();
    }

    public static <T extends CustomResource, L extends DefaultKubernetesResourceList<T>> void replaceCrdResource(String namespaceName, Class<T> crdClass, Class<L> listClass, String resourceName, Consumer<T> editor) {
        T toBeReplaced = Crds.operation(kubeClient().getClient(), crdClass, listClass).inNamespace(namespaceName).withName(resourceName).get();
        editor.accept(toBeReplaced);
        Crds.operation(kubeClient().getClient(), crdClass, listClass).inNamespace(namespaceName).resource(toBeReplaced).update();
    }

    /**
     * Wait until the CR is in desired state
     * @param operation - client of CR - for example kafkaClient()
     * @param resource - custom resource
     * @param statusType - desired status
     * @return returns CR
     */
    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> statusType, long resourceTimeout) {
        return waitForResourceStatus(resource.getMetadata().getNamespace(), operation, resource.getKind(), resource.getMetadata().getName(), statusType, ConditionStatus.True, resourceTimeout);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(String namespaceName, MixedOperation<T, ?, ?> operation, String kind, String name, Enum<?> statusType, long resourceTimeoutMs) {
        return waitForResourceStatus(namespaceName, operation, kind, name, statusType, ConditionStatus.True, resourceTimeoutMs);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(String namespaceName, MixedOperation<T, ?, ?> operation, String kind, String name, Enum<?> statusType, ConditionStatus conditionStatus, long resourceTimeoutMs) {
        LOGGER.log(ResourceManager.getInstance().determineLogLevel(), "Waiting for {}: {}/{} will have desired state: {}", kind, namespaceName, name, statusType);

        TestUtils.waitFor(String.format("%s: %s#%s will have desired state: %s", kind, namespaceName, name, statusType),
            TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, resourceTimeoutMs,
            () -> operation.inNamespace(namespaceName)
                .withName(name)
                .get().getStatus().getConditions().stream().anyMatch(condition -> condition.getType().equals(statusType.toString()) && condition.getStatus().equals(conditionStatus.toString()))
        );

        LOGGER.log(ResourceManager.getInstance().determineLogLevel(), "{}: {}/{} is in desired state: {}", kind, namespaceName, name, statusType);
        return true;
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> status) {
        long resourceTimeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());
        return waitForResourceStatus(operation, resource, status, resourceTimeout);
    }

    public static void waitForResourceReadiness(String namespaceName, String resourceType, String resourceName) {
        LOGGER.info("Waiting for " + resourceType + "/" + resourceName + " readiness");

        TestUtils.waitFor("readiness of resource " + resourceType + "/" + resourceName,
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_CMD_CLIENT_TIMEOUT,
            () -> ResourceManager.cmdKubeClient().namespace(namespaceName).getResourceReadiness(resourceType, resourceName));
        LOGGER.info("Resource " + resourceType + "/" + resourceName + " in namespace:" + namespaceName + " is ready");
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatusMessage(MixedOperation<T, ?, ?> operation, T resource, String message) {
        long resourceTimeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());
        return waitForResourceStatusMessage(operation, resource, message, resourceTimeout);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatusMessage(MixedOperation<T, ?, ?> operation, T resource, String message, long resourceTimeout) {
        waitForResourceStatusMessage(resource.getMetadata().getNamespace(), operation, resource.getKind(), resource.getMetadata().getName(), message, resourceTimeout);
        return true;
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatusMessage(String namespaceName, MixedOperation<T, ?, ?> operation, String kind, String name, String message, long resourceTimeoutMs) {
        LOGGER.info("Waiting for {}: {}/{} will contain desired status message: {}", kind, namespaceName, name, message);

        TestUtils.waitFor(String.format("%s: %s#%s will contain desired status message: %s", kind, namespaceName, name, message),
            TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, resourceTimeoutMs,
            () -> operation.inNamespace(namespaceName)
                .withName(name)
                .get().getStatus().getConditions().stream().anyMatch(condition -> condition.getMessage().contains(message) && condition.getStatus().equals("True"))
        );

        LOGGER.info("{}: {}/{} contains desired message in status: {}", kind, namespaceName, name, message);
        return true;
    }

    /**
     * Determines the appropriate log level based on the presence of the PERFORMANCE tag
     * in the current test context. Uses DEBUG level for performance tests and INFO level
     * for other tests.
     *
     * @return          The appropriate logging level (DEBUG or INFO) for the current test context.
     */
    public Level determineLogLevel() {
        return ResourceManager.getTestContext().getTags().contains(TestTags.PERFORMANCE) ? Level.DEBUG : Level.INFO;
    }
}
