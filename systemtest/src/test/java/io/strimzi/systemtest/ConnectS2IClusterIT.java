/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.ConnectS2ICluster;
import io.strimzi.test.KafkaFromClasspathYaml;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.StrimziRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

@RunWith(StrimziRunner.class)
@Namespace(ConnectS2IClusterIT.NAMESPACE)
@ClusterOperator
@KafkaFromClasspathYaml
public class ConnectS2IClusterIT extends AbstractClusterIT {

    public static final String NAMESPACE = "connect-s2i-cluster-test";
    public static final String CONNECT_CLUSTER_NAME = "my-cluster";
    public static final String KAFKA_CLUSTER_NAME = "connect-s2i-tests";
    public static final String KAFKA_CONNECT_BOOTSTRAP_SERVERS = KAFKA_CLUSTER_NAME + "-kafka:9092";
    public static final String CONNECT_CONFIG_CONVERTER_SCHEMAS_DISABLED = "{\n" +
            "\"bootstrap.servers\": \"" + KAFKA_CONNECT_BOOTSTRAP_SERVERS + "\", " +
            "\"key.converter.schemas.enable\": \"" + "false" + "\", " +
            "\"value.converter.schemas.enable\": \"" + "false" + "\"" +
            "}";

    @Test
    @OpenShiftOnly
//    @JUnitGroup(name = "regression")
    @ConnectS2ICluster(name = CONNECT_CLUSTER_NAME, connectConfig = CONNECT_CONFIG_CONVERTER_SCHEMAS_DISABLED)
    public void testDeployS2IWithMongoDBPlugin() {
        String pathToDebeziumMongodb = "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/0.3.0/debezium-connector-mongodb-0.3.0-plugin.tar.gz";
        // Create directory for plugin
        kubeClient.exec("mkdir", "-p", "./my-plugins/");
        // Download and unzip MongoDB plugin
        kubeClient.exec("wget", "-O", "debezium-connector-mongodb-plugin.tar.gz", "-P", "./my-plugins/", pathToDebeziumMongodb);
        kubeClient.exec("tar", "xf", "debezium-connector-mongodb-plugin.tar.gz", "-C", "./my-plugins/");
        // Start a new image build using the plugins directory
        kubeClient.exec("oc", "start-build", "my-cluster-connect", "--from-dir", "./my-plugins/");

        kubeClient.waitForDeploymentConfig(CONNECT_CLUSTER_NAME + "-connect");

        String connectS2IPodName = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect-s2i").get(0);
        String plugins = kubeClient.execInPod(connectS2IPodName, "curl", "-X", "GET", "http://localhost:8083/connector-plugins").out();

        assertThat(plugins, containsString("io.debezium.connector.mongodb.MongoDbConnector"));

//        waitFor("Wait message in pod log", 5000, 60000,
//            () -> !kubeClient.searchInLog("deploymentConfig", "my-cluster-connect", stopwatch.runtime(SECONDS), "\"Added plugin \'io.debezium.connector.mongodb.MongoDbConnector\'\"").isEmpty());
    }
}
