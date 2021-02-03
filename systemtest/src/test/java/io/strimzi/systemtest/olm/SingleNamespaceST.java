/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.systemtest.resources.operator.OlmResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.OLM;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag(OLM)
public class SingleNamespaceST extends OlmAbstractST {

    public static final String NAMESPACE = "olm-namespace";

    private static final Logger LOGGER = LogManager.getLogger(SingleNamespaceST.class);

    @Test
    @Order(1)
    void testDeployExampleKafka(ExtensionContext extensionContext) {
        doTestDeployExampleKafka();
    }

    @Test
    @Order(2)
    void testDeployExampleKafkaUser(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaUser(extensionContext);
    }

    @Test
    @Order(3)
    void testDeployExampleKafkaTopic(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaTopic();
    }

    @Test
    @Order(4)
    void testDeployExampleKafkaConnect(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaConnect();
    }

    @Test
    @Order(5)
    void testDeployExampleKafkaConnectS2I(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaConnectS2I();
    }

    @Test
    @Order(6)
    void testDeployExampleKafkaBridge(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaBridge();
    }

    @Test
    @Order(7)
    void testDeployExampleKafkaMirrorMaker(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaMirrorMaker();
    }

    @Test
    @Order(8)
    void testDeployExampleKafkaMirrorMaker2(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaMirrorMaker2();
    }

    @Test
    @Order(9)
    void testDeployExampleKafkaRebalance(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaRebalance(extensionContext);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        cluster.setNamespace(NAMESPACE);
        cluster.createNamespace(NAMESPACE);
        resourceManager.createResource(extensionContext, OlmResource.clusterOperator(NAMESPACE));
    }
}
