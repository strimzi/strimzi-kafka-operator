/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube2;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube2.controllers.AbstractMockController;
import io.strimzi.test.mockkube2.controllers.MockDeploymentController;
import io.strimzi.test.mockkube2.controllers.MockPodController;
import io.strimzi.test.mockkube2.controllers.MockServiceController;
import io.strimzi.test.mockkube2.controllers.MockStatefulSetController;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static io.strimzi.test.mockkube2.logging.JulLoggingConfiguration.configureJulLogging;

/**
 * MockKube2 is a utility class which helps to use the Fabric8 Kubernetes Mock Server. It provides methods to easily
 * create and register CRDs and to create mock controllers which emulate usual Kubernetes functionality useful for the
 * tests such as creating Pods when StatefulSet is created etc. When using MockKube2, call the stop() method to stop the
 * running controllers.
 */
public class MockKube2 {
    private final KubernetesClient client;
    private final List<AbstractMockController> controllers = new ArrayList<>();

    /**
     * Constructs the Kubernetes Mock Kube Server
     *
     * @param client    Kubernetes client backed by the Fabric8 Kubernetes Mock Server
     */
    public MockKube2(KubernetesClient client) {
        this.client = client;
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
     * @param apiVersion    API version of the Custom Resource
     * @param kind          Kind of the Custom Resource
     * @param crdClass      Class with the Custom Resource model
     * @param crdPath       Path to the YAML with the CRD definition
     */
    private void registerCrd(String apiVersion, String kind, Class<? extends KubernetesResource> crdClass, String crdPath)  {
        KubernetesDeserializer.registerCustomKind(apiVersion, kind, crdClass);

        CustomResourceDefinition crd = client
                .apiextensions().v1()
                .customResourceDefinitions()
                .load(crdPath)
                .get();

        client.apiextensions().v1().customResourceDefinitions().resource(crd).create();
    }

    /**
     * Starts the registered mock controllers
     */
    public void start() {
        for (AbstractMockController controller : controllers)   {
            controller.start();
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
    }

    /**
     * Builder used to build the MockKube instance
     */
    public static class MockKube2Builder {
        private final KubernetesClient client;
        private final MockKube2 mock;

        /**
         * Constructs the Kubernetes Mock Kube Builder
         *
         * @param client    Kubernetes client backed by the Fabric8 Kubernetes Mock Server
         */
        public MockKube2Builder(KubernetesClient client) {
            this.client = client;
            this.mock = new MockKube2(client);
        }

        /**
         * Registers deployment controller to manage Kubernetes Deployments
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withDeploymentController()  {
            mock.registerController(new MockDeploymentController(client));
            return this;
        }

        /**
         * Registers stateful set controller to manage Kubernetes StatefulSets
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withStatefulSetController()  {
            mock.registerController(new MockStatefulSetController(client));
            return this;
        }

        /**
         * Registers pod controller to manage Kubernetes Pods
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withPodController()  {
            mock.registerController(new MockPodController(client));
            return this;
        }

        /**
         * Registers service controller to manage Kubernetes Services
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withServiceController()  {
            mock.registerController(new MockServiceController(client));
            return this;
        }

        /**
         * Registers the Kafka CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaCrd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "Kafka", Kafka.class, TestUtils.CRD_KAFKA);
            return this;
        }

        /**
         * Registers the KafkaTopic CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaTopicCrd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "KafkaTopic", KafkaTopic.class, TestUtils.CRD_TOPIC);
            return this;
        }

        /**
         * Registers the KafkaUser CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaUserCrd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "KafkaUser", KafkaUser.class, TestUtils.CRD_KAFKA_USER);
            return this;
        }

        /**
         * Registers the KafkaConnect CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaConnectCrd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "KafkaConnect", KafkaConnect.class, TestUtils.CRD_KAFKA_CONNECT);
            return this;
        }

        /**
         * Registers the KafkaConnector CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaConnectorCrd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "KafkaConnector", KafkaConnector.class, TestUtils.CRD_KAFKA_CONNECTOR);
            return this;
        }

        /**
         * Registers the KafkaMirrorMaker2 CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaMirrorMaker2Crd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "KafkaMirrorMaker2", KafkaMirrorMaker2.class, TestUtils.CRD_KAFKA_MIRROR_MAKER_2);
            return this;
        }

        /**
         * Registers the KafkaRebalance CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withKafkaRebalanceCrd()  {
            mock.registerCrd("kafka.strimzi.io/v1beta2", "KafkaRebalance", KafkaRebalance.class, TestUtils.CRD_KAFKA_REBALANCE);
            return this;
        }

        /**
         * Registers the StrimziPodSet CRD
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withStrimziPodSetCrd()  {
            mock.registerCrd("core.strimzi.io/v1beta2", "StrimziPodSet", StrimziPodSet.class, TestUtils.CRD_STRIMZI_POD_SET);
            return this;
        }

        /**
         * Creates initial instances of the Kafka CR
         *
         * @param resources One or more custom resources
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withInitialKafkas(Kafka... resources)  {
            initializeResources(Crds.kafkaOperation(client), resources);
            return this;
        }

        /**
         * Creates initial instances of the KafkaConnect CR
         *
         * @param resources One or more custom resources
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withInitialKafkaConnects(KafkaConnect... resources)  {
            initializeResources(Crds.kafkaConnectOperation(client), resources);
            return this;
        }

        /**
         * Creates initial instances of the KafkaMirrorMaker2 CR
         *
         * @param resources One or more custom resources
         *
         * @return  MockKube builder instance
         */
        public MockKube2Builder withInitialKafkaMirrorMaker2s(KafkaMirrorMaker2... resources)  {
            initializeResources(Crds.kafkaMirrorMaker2Operation(client), resources);
            return this;
        }

        /**
         * Set the mock web server's logging level as needed, defaults to INFO
         * @param level logging level for mock web server
         *
         * @return MockKube builder instance
         */
        @SuppressWarnings("unused")
        public MockKube2Builder withMockWebServerLoggingSettings(Level level) {
            return withMockWebServerLoggingSettings(level, false);
        }

        /**
         * Set the mock web server's logging level and output stream
         * @param level defaults to INFO
         * @param logToStdOut defaults to false, which maintains consistency with existing behaviour which logs to stderr
         * @return MockKube builder instance
         */
        public MockKube2Builder withMockWebServerLoggingSettings(Level level, boolean logToStdOut) {
            configureJulLogging(level, logToStdOut);
            return this;
        }

        /**
         * Create an instance of the custom resource in the Kubernetes mock server
         *
         * @param op        Kubernetes client operation for working with given custom resource
         * @param resources Custom Resources which should be created
         * @param <T>       The type of the Custom Resource which should be created
         * @param <L>       The type of the Custom Resource List which should be created
         */
        @SafeVarargs
        @SuppressWarnings({ "rawtypes" })
        private <T extends CustomResource, L extends DefaultKubernetesResourceList<T>> void initializeResources(MixedOperation<T, L, Resource<T>> op, T... resources)   {
            for (T resource : resources)  {
                op.inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
            }
        }

        /**
         * Builds an instance of MockKube based on the builder configuration
         *
         * @return  MockKube instance
         */
        public MockKube2 build()   {
            return mock;
        }
    }
}
