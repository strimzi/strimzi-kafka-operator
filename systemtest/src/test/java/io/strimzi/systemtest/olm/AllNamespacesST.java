/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.systemtest.resources.specific.OlmResource;
import org.junit.jupiter.api.AfterAll;
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
public class AllNamespacesST extends OlmAbstractST {

    public static final String NAMESPACE = "olm-namespace";
    public static OlmResource olmResource;

    @Test
    @Order(1)
    void testDeployExampleKafka() {
        doTestDeployExampleKafka();
    }

    @Test
    @Order(2)
    void testDeployExampleKafkaUser(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaUser(extensionContext);
    }

    @Test
    @Order(3)
    void testDeployExampleKafkaTopic() {
        doTestDeployExampleKafkaTopic();
    }

    @Test
    @Order(4)
    void testDeployExampleKafkaConnect() {
        doTestDeployExampleKafkaConnect();
    }

    @Test
    @Order(5)
    void testDeployExampleKafkaConnectS2I() {
        doTestDeployExampleKafkaConnectS2I();
    }

    @Test
    @Order(6)
    void testDeployExampleKafkaBridge() {
        doTestDeployExampleKafkaBridge();
    }

    @Test
    @Order(7)
    void testDeployExampleKafkaMirrorMaker() {
        doTestDeployExampleKafkaMirrorMaker();
    }

    @Test
    @Order(8)
    void testDeployExampleKafkaMirrorMaker2() {
        doTestDeployExampleKafkaMirrorMaker2();
    }

    @Test
    @Order(9)
    void testDeployExampleKafkaRebalance(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaRebalance(extensionContext);
    }

    @BeforeAll
    void setup() {
        cluster.setNamespace(cluster.getDefaultOlmNamespace());

        olmResource = new OlmResource(cluster.getDefaultOlmNamespace());
        olmResource.create();

        cluster.setNamespace(NAMESPACE);
        cluster.createNamespace(NAMESPACE);
    }

    @AfterAll
    void tearDown() {
        olmResource.delete();
    }
}
