/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.systemtest.resources.operator.OlmResource;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AllNamespacesST extends OlmAbstractST {

    public static final String NAMESPACE = "olm-namespace";

    @Test
    @Order(1)
    void testDeployExampleKafka() {
        doTestDeployExampleKafka();
    }

    @Test
    @Order(2)
    void testDeployExampleKafkaUser() {
        doTestDeployExampleKafkaUser();
    }

    @Test
    @Order(3)
    void testDeployExampleKafkaTopic() {
        doTestDeployExampleKafkaTopic();
    }

    @Test
    @Tag(CONNECT)
    @Order(4)
    void testDeployExampleKafkaConnect() {
        doTestDeployExampleKafkaConnect();
    }

    @Test
    @Tag(CONNECT_S2I)
    @Order(5)
    void testDeployExampleKafkaConnectS2I() {
        doTestDeployExampleKafkaConnectS2I();
    }

    @Test
    @Tag(BRIDGE)
    @Order(6)
    void testDeployExampleKafkaBridge() {
        doTestDeployExampleKafkaBridge();
    }

    @Test
    @Tag(MIRROR_MAKER)
    @Order(7)
    void testDeployExampleKafkaMirrorMaker() {
        doTestDeployExampleKafkaMirrorMaker();
    }

    @Test
    @Tag(MIRROR_MAKER2)
    @Order(8)
    void testDeployExampleKafkaMirrorMaker2() {
        doTestDeployExampleKafkaMirrorMaker2();
    }


    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        cluster.setNamespace(cluster.getDefaultOlmNamespace());
        OlmResource.clusterOperator(cluster.getDefaultOlmNamespace());
        cluster.setNamespace(NAMESPACE);
        cluster.createNamespace(NAMESPACE);
    }
}
