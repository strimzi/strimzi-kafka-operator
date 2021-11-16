/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.systemtest.resources.operator.specific.OlmResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.OLM;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag(OLM)
public class SingleNamespaceST extends OlmAbstractST {

    public static final String NAMESPACE = "olm-namespace";

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
    @Tag(CONNECT)
    void testDeployExampleKafkaConnect() {
        doTestDeployExampleKafkaConnect();
    }

    @Test
    @Order(5)
    @Tag(BRIDGE)
    void testDeployExampleKafkaBridge() {
        doTestDeployExampleKafkaBridge();
    }

    @Test
    @Order(6)
    @Tag(MIRROR_MAKER)
    void testDeployExampleKafkaMirrorMaker() {
        doTestDeployExampleKafkaMirrorMaker();
    }

    @Test
    @Order(7)
    @Tag(MIRROR_MAKER2)
    void testDeployExampleKafkaMirrorMaker2() {
        doTestDeployExampleKafkaMirrorMaker2();
    }

    @Test
    @Order(8)
    void testDeployExampleKafkaRebalance(ExtensionContext extensionContext) {
        doTestDeployExampleKafkaRebalance(extensionContext);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        cluster.setNamespace(NAMESPACE);
        cluster.createNamespace(NAMESPACE);

        olmResource = new OlmResource(NAMESPACE);
        olmResource.create(extensionContext);
    }
}
