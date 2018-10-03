/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.StrimziRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

@RunWith(StrimziRunner.class)
@Namespace(ConnectS2IST.NAMESPACE)
@ClusterOperator
public class ConnectS2IST extends AbstractST {

    public static final String NAMESPACE = "connect-s2i-cluster-test";
    public static final String CONNECT_CLUSTER_NAME = "connect-s2i-tests";
    public static final String CONNECT_DEPLOYMENT_NAME = CONNECT_CLUSTER_NAME + "-connect";
    private static final Logger LOGGER = LogManager.getLogger(ConnectS2IST.class);
    private static Resources classResources;

    @Test
    @OpenShiftOnly
    @JUnitGroup(name = "regression")
    public void testDeployS2IWithMongoDBPlugin() {
        resources().kafkaConnectS2I(CONNECT_CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect-s2i")
            .endMetadata()
            .done();

        String pathToDebeziumMongodb = "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/0.3.0/debezium-connector-mongodb-0.3.0-plugin.tar.gz";
        // Create directory for plugin
        kubeClient.exec("mkdir", "-p", "./my-plugins/");
        // Download and unzip MongoDB plugin
        kubeClient.exec("wget", "-O", "debezium-connector-mongodb-plugin.tar.gz", "-P", "./my-plugins/", pathToDebeziumMongodb);
        kubeClient.exec("tar", "xf", "debezium-connector-mongodb-plugin.tar.gz", "-C", "./my-plugins/");

        String connectS2IPodName = kubeClient.listResourcesByLabel("pod", "type=kafka-connect-s2i").get(0);

        // Start a new image build using the plugins directory
        kubeClient.exec("oc", "start-build", CONNECT_DEPLOYMENT_NAME, "--from-dir", "./my-plugins/");
        kubeClient.waitForResourceDeletion("pod", connectS2IPodName);

        kubeClient.waitForDeploymentConfig(CONNECT_DEPLOYMENT_NAME);

        connectS2IPodName = kubeClient.listResourcesByLabel("pod", "type=kafka-connect-s2i").get(0);
        String plugins = kubeClient.execInPod(connectS2IPodName, "curl", "-X", "GET", "http://localhost:8083/connector-plugins").out();

        assertThat(plugins, containsString("io.debezium.connector.mongodb.MongoDbConnector"));
    }


    @BeforeClass
    public static void createClassResources() {
        classResources = new Resources(namespacedClient());
        classResources().kafkaEphemeral(CONNECT_CLUSTER_NAME, 3).done();
    }

    @AfterClass
    public static void deleteClassResources() {
        LOGGER.info("Deleting resources after the test class");
        classResources.deleteResources();
        classResources = null;
    }

    static Resources classResources() {
        return classResources;
    }
}
