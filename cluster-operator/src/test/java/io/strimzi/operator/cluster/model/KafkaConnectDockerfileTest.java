/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.connect.build.Artifact;
import io.strimzi.api.kafka.model.connect.build.Build;
import io.strimzi.api.kafka.model.connect.build.BuildBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import org.junit.jupiter.api.Test;

import static io.strimzi.operator.cluster.model.KafkaBrokerConfigurationBuilderTest.IsEquivalent.isEquivalent;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaConnectDockerfileTest {
    private final Artifact jarArtifactNoChecksum = new JarArtifactBuilder()
            .withUrl("https://mydomain.tld/my.jar")
            .build();

    private final Artifact jarArtifactWithChecksum = new JarArtifactBuilder()
            .withUrl("https://mydomain.tld/my2.jar")
            .withSha512sum("sha-512-checksum")
            .build();

    @Test
    public void testEmptyDockerfile()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(emptyList())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "USER 1001"));
    }

    @Test
    public void testNoArtifacts()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(emptyList())
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "USER 1001"));
    }

    @Test
    public void testNoChecksumJarArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(jarArtifactNoChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/51e5038c \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/51e5038c/my.jar https://mydomain.tld/my.jar",
                "USER 1001"));
    }

    @Test
    public void testChecksumJarArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(jarArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/0df6d15c \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar https://mydomain.tld/my2.jar \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar\" > /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512",
                "USER 1001"));
    }

    @Test
    public void testMultipleJarArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(jarArtifactNoChecksum, jarArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/51e5038c \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/51e5038c/my.jar https://mydomain.tld/my.jar",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/0df6d15c \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar https://mydomain.tld/my2.jar \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar\" > /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512",
                "USER 1001"));
    }

    @Test
    public void testDockerfileWithComments()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(jarArtifactNoChecksum, jarArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), is("##############################\n" +
                "##############################\n" +
                "# This file is automatically generated by the Strimzi Cluster Operator\n" +
                "# Any changes to this file will be ignored and overwritten!\n" +
                "##############################\n" +
                "##############################\n" +
                "\n" +
                "FROM myImage:latest\n" +
                "\n" +
                "USER root:root\n" +
                "\n" +
                "##########\n" +
                "# Connector plugin my-connector-plugin\n" +
                "##########\n" +
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/51e5038c \\\n" +
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/51e5038c/my.jar https://mydomain.tld/my.jar\n" +
                "\n" +
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/0df6d15c \\\n" +
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar https://mydomain.tld/my2.jar \\\n" +
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar\" > /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512 \\\n" +
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512 \\\n" +
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/0df6d15c/my2.jar.sha512\n" +
                "\n" +
                "USER 1001\n\n"));
    }
}
