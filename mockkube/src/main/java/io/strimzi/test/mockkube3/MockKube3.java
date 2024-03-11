/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3;

import com.dajudge.kindcontainer.ApiServerContainer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube3.controllers.AbstractMockController;
import io.strimzi.test.mockkube3.controllers.MockDeletionController;
import io.strimzi.test.mockkube3.controllers.MockDeploymentController;
import io.strimzi.test.mockkube3.controllers.MockPodController;
import io.strimzi.test.mockkube3.controllers.MockServiceController;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MockKube3 is a utility class which helps to test Strimzi components. It is using an actual Kubernetes API Server
 * (with Etcd), but without the rest of Kubernetes. So it does not run the actual Pods etc. It also has utility methods
 * to create and register CRDs and to create mock controllers which emulate usual Kubernetes functionality useful for the
 * tests such as creating Pods when Deployment is created etc. When using MockKube3, call the stop() method to stop the
 * running controllers.
 */
public class MockKube3 {
    private final static String ETCD_IMAGE = "registry.k8s.io/etcd:3.5.12-0";

    private final ApiServerContainer<?> apiServer;
    private final List<AbstractMockController> controllers = new ArrayList<>();
    private final List<String> crds = new ArrayList<>();
    private final List<String> initialNamespaces = new ArrayList<>();
    private final List<Kafka> initialKafkas = new ArrayList<>();
    private final List<KafkaNodePool> initialKafkaNodePools = new ArrayList<>();

    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // This field is initialized in the start method after the API server is started
    private KubernetesClient client;

    /**
     * Constructs the Kubernetes Mock Kube Server
     */
    public MockKube3() {
        // Override the Etcd version to get multiplatform support
        this.apiServer = new ApiServerContainer<>().withEtcdImage(DockerImageName.parse(ETCD_IMAGE));
    }

    /**
     * Register mock controller so that we know about it and can start it or shut it down at the end of the test (from
     * the stop() method)
     *
     * @param controller    Mock controller which should be registered
     */
    private void registerController(AbstractMockController controller)   {
        controllers.add(controller);
    }

    /**
     * Registers Custom Resource definition in the mock Kubernetes cluster. This registers it for deserialization, but
     * also creates the CRD in the Kubernetes server.
     *
     * @param crdPath   Path to the YAML with the CRD definition
     */
    private void registerCrd(String crdPath)  {
        crds.add(crdPath);
    }

    /**
     * @return  Returns the Kubernetes client connected to this Mock Kube server
     */
    public KubernetesClient client()   {
        if (client != null) {
            return client;
        } else {
            throw new RuntimeException("Kubernetes client is not available. The MocKube might not be running.");
        }
    }

    /**
     * Starts the registered mock controllers
     */
    public void start() {
        apiServer.start();

        // Create the client
        Config clientConfig = Config.fromKubeconfig(apiServer.getKubeconfig());
        client = new KubernetesClientBuilder().withConfig(clientConfig).build();

        initializeNamespaces();
        createCrds();
        initializeKafkas();
        initializeKafkaNodePools();
        startControllers();
    }

    /**
     * Creates the registered CRDs from the paths to their definitions
     */
    private void createCrds()    {
        for (String crdPath : crds) {
            client.apiextensions().v1()
                    .customResourceDefinitions()
                    .load(crdPath)
                    .create();
        }

        for (String crdPath : crds) {
            client.apiextensions().v1()
                    .customResourceDefinitions()
                    .load(crdPath)
                    .waitUntilCondition(MockKube3::isCrdEstablished, 10, TimeUnit.SECONDS);
        }
    }

    private static boolean isCrdEstablished(CustomResourceDefinition crd)   {
        return crd.getStatus() != null
                && crd.getStatus().getConditions() != null
                && crd.getStatus().getConditions().stream().anyMatch(c -> "Established".equals(c.getType()) && "True".equals(c.getStatus()));
    }

    /**
     * Creates the initial namespaces
     */
    private void initializeNamespaces() {
        for (String namespace : initialNamespaces)  {
            client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build()).create();
            // Each namespace has a default service account
            client.serviceAccounts().inNamespace(namespace).resource(new ServiceAccountBuilder().withNewMetadata().withName("default").withNamespace(namespace).endMetadata().build()).create();
        }
    }

    /**
     * Creates the initial namespaces
     *
     * @param namespace     Namespace name
     */
    public void prepareNamespace(String namespace) {
        client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build()).create();
        // Each namespace has a default service account
        client.serviceAccounts().inNamespace(namespace).resource(new ServiceAccountBuilder().withNewMetadata().withName("default").withNamespace(namespace).endMetadata().build()).create();
    }

    /**
     * Creates the initial Kafka resources
     */
    private void initializeKafkas() {
        initializeResources(Crds.kafkaOperation(client), initialKafkas);
    }

    /**
     * Creates the initial KafkaNodePool resources
     */
    private void initializeKafkaNodePools() {
        initializeResources(Crds.kafkaNodePoolOperation(client), initialKafkaNodePools);
    }

    /**
     * Create an instance of the custom resource in the Kubernetes server
     *
     * @param op        Kubernetes client operation for working with given custom resource
     * @param resources Custom Resources which should be created
     * @param <T>       The type of the Custom Resource which should be created
     * @param <L>       The type of the Custom Resource List which should be created
     */
    @SuppressWarnings({ "rawtypes" })
    private <T extends CustomResource, L extends DefaultKubernetesResourceList<T>> void initializeResources(MixedOperation<T, L, Resource<T>> op, List<T> resources)   {
        for (T resource : resources)  {
            op.inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
        }
    }

    /**
     * Starts the registered controllers
     */
    private void startControllers() {
        for (AbstractMockController controller : controllers)   {
            controller.start(client);
        }
    }

    /**
     * Stops the registered controllers. This should be called at the end of the tests before the mock Kubernetes server
     * is shut down.
     */
    public void stop() {
        for (AbstractMockController controller : controllers)   {
            controller.stop();
        }

        apiServer.stop();
    }

    /**
     * Builder used to build the MockKube instance
     */
    public static class MockKube3Builder {
        private final MockKube3 mock;

        /**
         * Constructs the Kubernetes Mock Kube Builder
         */
        public MockKube3Builder() {
            this.mock = new MockKube3();
        }

        /**
         * Registers deployment controller to manage Kubernetes Deployments
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withDeploymentController()  {
            mock.registerController(new MockDeploymentController());
            return this;
        }

        /**
         * Registers pod controller to manage Kubernetes Pods
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withPodController()  {
            mock.registerController(new MockPodController());
            return this;
        }

        /**
         * Registers service controller to manage Kubernetes Services
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withServiceController()  {
            mock.registerController(new MockServiceController());
            return this;
        }

        /**
         * Registers deletion controller that handles deletion of various resources
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withDeletionController()  {
            mock.registerController(new MockDeletionController());
            return this;
        }

        /**
         * Registers the Kafka CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaCrd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA);
            return this;
        }

        /**
         * Registers the KafkaTopic CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaTopicCrd()  {
            mock.registerCrd(TestUtils.CRD_TOPIC);
            return this;
        }

        /**
         * Registers the KafkaUser CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaUserCrd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA_USER);
            return this;
        }

        /**
         * Registers the KafkaConnect CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaConnectCrd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA_CONNECT);
            return this;
        }

        /**
         * Registers the KafkaConnector CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaConnectorCrd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA_CONNECTOR);
            return this;
        }

        /**
         * Registers the KafkaMirrorMaker2 CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaMirrorMaker2Crd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA_MIRROR_MAKER_2);
            return this;
        }

        /**
         * Registers the KafkaRebalance CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaRebalanceCrd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA_REBALANCE);
            return this;
        }

        /**
         * Registers the KafkaNodePool CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withKafkaNodePoolCrd()  {
            mock.registerCrd(TestUtils.CRD_KAFKA_NODE_POOL);
            return this;
        }

        /**
         * Registers the StrimziPodSet CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withStrimziPodSetCrd()  {
            mock.registerCrd(TestUtils.CRD_STRIMZI_POD_SET);
            return this;
        }

        /**
         * Creates initial namespaces in the Kubernetes cluster
         *
         * @param namespaces    One or more namespaces that should be prepared in the cluster
         *
         * @return  MockKube builder instance
         */
        public MockKube3Builder withNamespaces(String... namespaces)  {
            mock.initialNamespaces.addAll(Arrays.stream(namespaces).toList());
            return this;
        }

        /**
         * Builds an instance of MockKube based on the builder configuration
         *
         * @return  MockKube instance
         */
        public MockKube3 build()   {
            return mock;
        }
    }
}
