/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentConfigUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ReplicaSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static io.strimzi.systemtest.Constants.CLUSTER_ROLE_BINDING;
import static io.strimzi.systemtest.Constants.DEPLOYMENT;
import static io.strimzi.systemtest.Constants.INGRESS;
import static io.strimzi.systemtest.Constants.ROLE_BINDING;
import static io.strimzi.systemtest.Constants.SERVICE;
import static java.util.Arrays.asList;

public class ResourceManager {

    private static final Logger LOGGER = LogManager.getLogger(ResourceManager.class);

    public static final String STRIMZI_PATH_TO_CO_CONFIG = TestUtils.USER_PATH + "/../install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml";
    public static final long CR_CREATION_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness();

    private static Stack<Runnable> classResources = new Stack<>();
    private static Stack<Runnable> methodResources = new Stack<>();
    private static Stack<Runnable> pointerResources = classResources;

    private static String coDeploymentName = Constants.STRIMZI_DEPLOYMENT_NAME;

    private static ResourceManager instance;

    public static synchronized ResourceManager getInstance() {
        if (instance == null) {
            instance = new ResourceManager();
        }
        return instance;
    }

    private ResourceManager() {}

    public static KubeClient kubeClient() {
        return KubeClusterResource.kubeClient();
    }

    public static KubeCmdClient cmdKubeClient() {
        return KubeClusterResource.cmdKubeClient();
    }

    public static HelmClient helmClient() {
        return KubeClusterResource.helmClusterClient();
    }

    public static Stack<Runnable> getPointerResources() {
        return pointerResources;
    }

    public static void setMethodResources() {
        LOGGER.info("Setting pointer to method resources");
        pointerResources = methodResources;
    }

    public static void setClassResources() {
        LOGGER.info("Setting pointer to class resources");
        pointerResources = classResources;
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>, D extends Doneable<T>> void replaceCrdResource(Class<T> crdClass, Class<L> listClass, Class<D> doneableClass, String resourceName, Consumer<T> editor) {
        Resource<T, D> namedResource = Crds.operation(kubeClient().getClient(), crdClass, listClass, doneableClass).inNamespace(kubeClient().getNamespace()).withName(resourceName);
        T resource = namedResource.get();
        editor.accept(resource);
        namedResource.replace(resource);
    }
    @SuppressWarnings("unchecked")
    public static <T extends HasMetadata> T deleteLater(MixedOperation<T, ?, ?, ?> operation, T resource) {
        LOGGER.debug("Scheduled deletion of {} {} in namespace {}",
                resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace() == null ? "(not set)" : resource.getMetadata().getNamespace());
        switch (resource.getKind()) {
            case Kafka.RESOURCE_KIND:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    waitForDeletion((Kafka) resource);
                });
                break;
            case KafkaConnect.RESOURCE_KIND:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    waitForDeletion((KafkaConnect) resource);
                });
                break;
            case KafkaConnectS2I.RESOURCE_KIND:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    waitForDeletion((KafkaConnectS2I) resource);
                });
                break;
            case KafkaMirrorMaker.RESOURCE_KIND:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    waitForDeletion((KafkaMirrorMaker) resource);
                });
                break;
            case KafkaMirrorMaker2.RESOURCE_KIND:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    waitForDeletion((KafkaMirrorMaker2) resource);
                });
                break;
            case KafkaBridge.RESOURCE_KIND:
                pointerResources.add(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    waitForDeletion((KafkaBridge) resource);
                });
                break;
            case DEPLOYMENT:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {}",
                            resource.getKind(), resource.getMetadata().getName());
                    waitForDeletion((Deployment) resource);
                });
                break;
            case CLUSTER_ROLE_BINDING:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {}",
                            resource.getKind(), resource.getMetadata().getName());
                    kubeClient().getClient().rbac().clusterRoleBindings().withName(resource.getMetadata().getName()).delete();
                });
                break;
            case ROLE_BINDING:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {}",
                            resource.getKind(), resource.getMetadata().getName());
                    kubeClient().getClient().rbac().roleBindings().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
                });
                break;
            case SERVICE:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    kubeClient().getClient().services().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
                });
                break;
            case INGRESS:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                    kubeClient().deleteIngress((Ingress) resource);
                });
                break;
            default:
                pointerResources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    operation.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                            .delete();
                });
        }
        return resource;
    }

    private static void waitForDeletion(Kafka kafka) {
        String kafkaClusterName = kafka.getMetadata().getName();
        LOGGER.info("Waiting when all the pods are terminated for Kafka {}", kafkaClusterName);

        StatefulSetUtils.waitForStatefulSetDeletion(KafkaResources.zookeeperStatefulSetName(kafkaClusterName));

        IntStream.rangeClosed(0, kafka.getSpec().getZookeeper().getReplicas() - 1).forEach(podIndex ->
                PodUtils.deletePodWithWait(KafkaResources.zookeeperPodName(kafka.getMetadata().getName(), podIndex)));

        StatefulSetUtils.waitForStatefulSetDeletion(KafkaResources.kafkaStatefulSetName(kafkaClusterName));

        IntStream.rangeClosed(0, kafka.getSpec().getKafka().getReplicas() - 1).forEach(podIndex ->
                PodUtils.deletePodWithWait(KafkaResources.kafkaPodName(kafka.getMetadata().getName(), podIndex)));

        // Wait for EO deletion
        DeploymentUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(kafkaClusterName));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaResources.entityOperatorDeploymentName(kafkaClusterName));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().contains(KafkaResources.entityOperatorDeploymentName(kafka.getMetadata().getName())))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));

        // Wait for Kafka Exporter deletion
        DeploymentUtils.waitForDeploymentDeletion(KafkaExporterResources.deploymentName(kafkaClusterName));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaExporterResources.deploymentName(kafkaClusterName));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().contains(KafkaExporterResources.deploymentName(kafka.getMetadata().getName())))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));

        SecretUtils.waitForClusterSecretsDeletion(kafkaClusterName);
        PersistentVolumeClaimUtils.waitUntilPVCDeletion(kafkaClusterName);

        ConfigMapUtils.waitUntilConfigMapDeletion(KafkaResources.kafkaMetricsAndLogConfigMapName(kafka.getMetadata().getName()));
        ConfigMapUtils.waitUntilConfigMapDeletion(KafkaResources.zookeeperMetricsAndLogConfigMapName(kafka.getMetadata().getName()));
    }

    private static void waitForDeletion(KafkaConnect kafkaConnect) {
        LOGGER.info("Waiting when all the Pods are terminated for KafkaConnect {}", kafkaConnect.getMetadata().getName());

        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnect.getMetadata().getName()));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnect.getMetadata().getName()));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaConnectResources.deploymentName(kafkaConnect.getMetadata().getName())))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));
    }

    private static void waitForDeletion(KafkaConnectS2I kafkaConnectS2I) {
        LOGGER.info("Waiting when all the Pods are terminated for KafkaConnectS2I {}", kafkaConnectS2I.getMetadata().getName());

        DeploymentConfigUtils.waitForDeploymentConfigDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().contains("-connect-"))
                .forEach(p -> {
                    LOGGER.debug("Deleting: {}", p.getMetadata().getName());
                    kubeClient().deletePod(p);
                });
    }

    private static void waitForDeletion(KafkaMirrorMaker kafkaMirrorMaker) {
        LOGGER.info("Waiting when all the Pods are terminated for KafkaMirrorMaker {}", kafkaMirrorMaker.getMetadata().getName());

        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMaker.getMetadata().getName()));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMaker.getMetadata().getName()));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaMirrorMaker2Resources.deploymentName(kafkaMirrorMaker.getMetadata().getName())))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));
    }

    private static void waitForDeletion(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        LOGGER.info("Waiting when all the Pods are terminated for KafkaMirrorMaker2 {}", kafkaMirrorMaker2.getMetadata().getName());

        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMaker2.getMetadata().getName()));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMaker2.getMetadata().getName()));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaMirrorMaker2Resources.deploymentName(kafkaMirrorMaker2.getMetadata().getName())))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));
    }

    private static void waitForDeletion(KafkaBridge kafkaBridge) {
        LOGGER.info("Waiting when all the Pods are terminated for KafkaBridge {}", kafkaBridge.getMetadata().getName());

        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaBridge.getMetadata().getName()));
        ReplicaSetUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaBridge.getMetadata().getName()));

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaBridgeResources.deploymentName(kafkaBridge.getMetadata().getName())))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));
    }

    private static void waitForDeletion(Deployment deployment) {
        LOGGER.info("Waiting when all the pods are terminated for Deployment {}", deployment.getMetadata().getName());

        DeploymentUtils.waitForDeploymentDeletion(deployment.getMetadata().getName());

        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(deployment.getMetadata().getName()))
                .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));
    }

    public static void deleteClassResources() {
        LOGGER.info("-----CLEARING CLASS RESOURCES-----");
        while (!classResources.empty()) {
            classResources.pop().run();
        }
        classResources.clear();
        LOGGER.info("-----CLASS RESOURCES CLEARED-----");
    }

    public static void deleteMethodResources() {
        LOGGER.info("-----CLEARING METHOD RESOURCES-----");
        while (!methodResources.empty()) {
            methodResources.pop().run();
        }
        methodResources.clear();
        pointerResources = classResources;
        LOGGER.info("-----METHOD RESOURCES CLEARED-----");
    }

    public static String getImageValueFromCO(String name) {
        Deployment clusterOperator = getDeploymentFromYaml(STRIMZI_PATH_TO_CO_CONFIG);

        List<EnvVar> listEnvVar = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        Optional<EnvVar> envVar = listEnvVar.stream().filter(e -> e.getName().equals(name)).findFirst();
        if (envVar.isPresent()) {
            return envVar.get().getValue();
        }
        return "";
    }
    /**
     * Log actual status of custom resource with pods.
     * @param customResource - Kafka, KafkaConnect etc. - every resource that HasMetadata and HasStatus (Strimzi status)
     */
    public static <T extends HasMetadata & HasStatus> void logCurrentResourceStatus(T customResource) {
        if (customResource != null) {
            List<String> printWholeCR = Arrays.asList(KafkaConnector.RESOURCE_KIND, KafkaTopic.RESOURCE_KIND, KafkaUser.RESOURCE_KIND);

            String kind = customResource.getKind();
            String name = customResource.getMetadata().getName();

            if (printWholeCR.contains(kind)) {
                LOGGER.info(customResource);
            } else {
                List<String> log = new ArrayList<>(asList("\n", kind, " status:\n", "\nConditions:\n"));

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

                    for (Pod pod : kubeClient().listPodsByPrefixInName(name)) {
                        log.add(pod.getMetadata().getName() + ":");
                        for (PodCondition podCondition : pod.getStatus().getConditions()) {
                            if (podCondition.getMessage() != null) {
                                log.add("\n\tType: " + podCondition.getType() + "\n");
                                log.add("\tMessage: " + podCondition.getMessage() + "\n");
                            }
                        }
                        log.add("\n\n");
                    }
                    LOGGER.info("{}", String.join("", log));
                }
            }
        }
    }

    /**
     * Wait until the CR is in desired state
     * @param operation - client of CR - for example kafkaClient()
     * @param resource - custom resource
     * @param status - desired status
     * @return returns CR
     */
    public static <T extends HasMetadata & HasStatus> T waitForResourceStatus(MixedOperation<T, ?, ?, ?> operation, T resource, Enum<?> status, long resourceTimeout) {
        LOGGER.info("Wait for {}: {} will have desired state: {}", resource.getKind(), resource.getMetadata().getName(), status);

        TestUtils.waitFor(String.format("Wait for %s: %s will have desired state: %s", resource.getKind(), resource.getMetadata().getName(), status),
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, resourceTimeout,
            () -> operation.inNamespace(resource.getMetadata().getNamespace())
                    .withName(resource.getMetadata().getName())
                    .get().getStatus().getConditions().stream().anyMatch(condition -> condition.getType().equals(status.toString())),
            () -> logCurrentResourceStatus(operation.inNamespace(resource.getMetadata().getNamespace())
                    .withName(resource.getMetadata().getName())
                    .get()));

        LOGGER.info("{}: {} is in desired state: {}", resource.getKind(), resource.getMetadata().getName(), status);
        return resource;
    }

    public static <T extends HasMetadata & HasStatus> T waitForResourceStatus(MixedOperation<T, ?, ?, ?> operation, T resource, Enum<?> status) {
        long resourceTimeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());
        return waitForResourceStatus(operation, resource, status, resourceTimeout);
    }

    private static Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }

    public static String getCoDeploymentName() {
        return coDeploymentName;
    }

    public static void setCoDeploymentName(String newName) {
        coDeploymentName = newName;
    }
}
