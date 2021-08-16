/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.connect.build.Artifact;
import io.strimzi.api.kafka.model.connect.build.Build;
import io.strimzi.api.kafka.model.connect.build.BuildBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.MavenArtifact;
import io.strimzi.api.kafka.model.connect.build.MavenArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.OtherArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.TgzArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.ZipArtifactBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import static io.strimzi.operator.cluster.model.KafkaBrokerConfigurationBuilderTest.IsEquivalent.isEquivalent;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KafkaConnectDockerfileTest {
    private final Artifact jarArtifactNoChecksum = new JarArtifactBuilder()
            .withUrl("https://mydomain.tld/my.jar")
            .build();

    private final Artifact jarArtifactWithChecksum = new JarArtifactBuilder()
            .withUrl("https://mydomain.tld/my2.jar")
            .withSha512sum("sha-512-checksum")
            .build();

    private final Artifact otherArtifactNoChecksum = new OtherArtifactBuilder()
            .withUrl("https://mydomain.tld/download?artifact=my.so")
            .withFileName("my.so")
            .build();

    private final Artifact otherArtifactWithChecksum = new OtherArtifactBuilder()
            .withUrl("https://mydomain.tld/download?artifactId=1874")
            .withSha512sum("sha-512-checksum")
            .withFileName("my2.so")
            .build();

    private final Artifact otherArtifactWithChecksumWithoutName = new OtherArtifactBuilder()
            .withUrl("https://mydomain.tld/download?artifactId=1874")
            .withSha512sum("sha-512-checksum")
            .build();

    private final Artifact tgzArtifactNoChecksum = new TgzArtifactBuilder()
            .withUrl("https://mydomain.tld/my.tgz")
            .build();

    private final Artifact zipArtifactNoChecksum = new ZipArtifactBuilder()
            .withUrl("https://mydomain.tld/my.zip")
            .build();

    private final Artifact tgzArtifactWithChecksum = new TgzArtifactBuilder()
            .withUrl("https://mydomain.tld/my2.tgz")
            .withSha512sum("sha-512-checksum")
            .build();

    private final Artifact zipArtifactWithChecksum = new ZipArtifactBuilder()
            .withUrl("https://mydomain.tld/my2.zip")
            .withSha512sum("sha-512-checksum")
            .build();

    @ParallelTest
    public void testEmptyDockerfile()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(emptyList())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "USER 1001"));
    }

    @ParallelTest
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

    @ParallelTest
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
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/51e5038c/51e5038c.jar https://mydomain.tld/my.jar",
                "USER 1001"));
    }

    @ParallelTest
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
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar https://mydomain.tld/my2.jar \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar\" > /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512",
                "USER 1001"));
    }

    @ParallelTest
    public void testNoChecksumOtherArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(otherArtifactNoChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/2c3b64c7 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/2c3b64c7/my.so https://mydomain.tld/download?artifact=my.so",
                "USER 1001"));
    }

    @ParallelTest
    public void testChecksumOtherArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(otherArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/2e6fee06 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/2e6fee06/my2.so https://mydomain.tld/download?artifactId=1874 \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/2e6fee06/my2.so\" > /opt/kafka/plugins/my-connector-plugin/2e6fee06/my2.so.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/2e6fee06/my2.so.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/2e6fee06/my2.so.sha512",
                "USER 1001"));
    }

    @ParallelTest
    public void testChecksumOtherArtifactWithoutName()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(otherArtifactWithChecksumWithoutName)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/2e6fee06 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/2e6fee06/2e6fee06 https://mydomain.tld/download?artifactId=1874 \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/2e6fee06/2e6fee06\" > /opt/kafka/plugins/my-connector-plugin/2e6fee06/2e6fee06.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/2e6fee06/2e6fee06.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/2e6fee06/2e6fee06.sha512",
                "USER 1001"));
    }

    @ParallelTest
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
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/51e5038c/51e5038c.jar https://mydomain.tld/my.jar",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/0df6d15c \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar https://mydomain.tld/my2.jar \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar\" > /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512",
                "USER 1001"));
    }

    @ParallelTest
    public void testNoChecksumTgzArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(tgzArtifactNoChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/6718766b \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/6718766b.tgz https://mydomain.tld/my.tgz \\",
                "      && tar xvfz /opt/kafka/plugins/my-connector-plugin/6718766b.tgz -C /opt/kafka/plugins/my-connector-plugin/6718766b \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/6718766b.tgz",
                "USER 1001"));
    }

    @ParallelTest
    public void testNoChecksumZipArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(zipArtifactNoChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/d8d533bc \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/d8d533bc.zip https://mydomain.tld/my.zip \\",
                "      && unzip /opt/kafka/plugins/my-connector-plugin/d8d533bc.zip -d /opt/kafka/plugins/my-connector-plugin/d8d533bc \\",
                "      && find /opt/kafka/plugins/my-connector-plugin/d8d533bc -type l | xargs rm -f \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/d8d533bc.zip",
                "USER 1001"));
    }

    @ParallelTest
    public void testChecksumZipArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(zipArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/90e04094 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/90e04094.zip https://mydomain.tld/my2.zip \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/90e04094.zip\" > /opt/kafka/plugins/my-connector-plugin/90e04094.zip.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/90e04094.zip.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/90e04094.zip.sha512 \\",
                "      && unzip /opt/kafka/plugins/my-connector-plugin/90e04094.zip -d /opt/kafka/plugins/my-connector-plugin/90e04094 \\",
                "      && find /opt/kafka/plugins/my-connector-plugin/90e04094 -type l | xargs rm -f \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/90e04094.zip",
                "USER 1001"));
    }

    @ParallelTest
    public void testChecksumTgzArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(tgzArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/638bd501 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/638bd501.tgz https://mydomain.tld/my2.tgz \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/638bd501.tgz\" > /opt/kafka/plugins/my-connector-plugin/638bd501.tgz.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/638bd501.tgz.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/638bd501.tgz.sha512 \\",
                "      && tar xvfz /opt/kafka/plugins/my-connector-plugin/638bd501.tgz -C /opt/kafka/plugins/my-connector-plugin/638bd501 \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/638bd501.tgz",
                "USER 1001"));
    }

    @ParallelTest
    public void testMultipleTgzArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(tgzArtifactNoChecksum, tgzArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/6718766b \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/6718766b.tgz https://mydomain.tld/my.tgz \\",
                "      && tar xvfz /opt/kafka/plugins/my-connector-plugin/6718766b.tgz -C /opt/kafka/plugins/my-connector-plugin/6718766b \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/6718766b.tgz",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/638bd501 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/638bd501.tgz https://mydomain.tld/my2.tgz \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/638bd501.tgz\" > /opt/kafka/plugins/my-connector-plugin/638bd501.tgz.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/638bd501.tgz.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/638bd501.tgz.sha512 \\",
                "      && tar xvfz /opt/kafka/plugins/my-connector-plugin/638bd501.tgz -C /opt/kafka/plugins/my-connector-plugin/638bd501 \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/638bd501.tgz",
                "USER 1001"));
    }

    @ParallelTest
    public void testMultipleZipArtifact()   {
        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(zipArtifactNoChecksum, zipArtifactWithChecksum)
                        .build())
                .build();

        KafkaConnectDockerfile df = new KafkaConnectDockerfile("myImage:latest", connectBuild);

        assertThat(df.getDockerfile(), isEquivalent("FROM myImage:latest",
                "USER root:root",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/d8d533bc \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/d8d533bc.zip https://mydomain.tld/my.zip \\",
                "      && unzip /opt/kafka/plugins/my-connector-plugin/d8d533bc.zip -d /opt/kafka/plugins/my-connector-plugin/d8d533bc \\",
                "      && find /opt/kafka/plugins/my-connector-plugin/d8d533bc -type l | xargs rm -f \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/d8d533bc.zip",
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/90e04094 \\",
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/90e04094.zip https://mydomain.tld/my2.zip \\",
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/90e04094.zip\" > /opt/kafka/plugins/my-connector-plugin/90e04094.zip.sha512 \\",
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/90e04094.zip.sha512 \\",
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/90e04094.zip.sha512 \\",
                "      && unzip /opt/kafka/plugins/my-connector-plugin/90e04094.zip -d /opt/kafka/plugins/my-connector-plugin/90e04094 \\",
                "      && find /opt/kafka/plugins/my-connector-plugin/90e04094 -type l | xargs rm -f \\",
                "      && rm -vf /opt/kafka/plugins/my-connector-plugin/90e04094.zip",
                "USER 1001"));
    }

    @ParallelTest
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
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/51e5038c/51e5038c.jar https://mydomain.tld/my.jar\n" +
                "\n" +
                "RUN mkdir -p /opt/kafka/plugins/my-connector-plugin/0df6d15c \\\n" +
                "      && curl -L --output /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar https://mydomain.tld/my2.jar \\\n" +
                "      && echo \"sha-512-checksum /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar\" > /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512 \\\n" +
                "      && sha512sum --check /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512 \\\n" +
                "      && rm -f /opt/kafka/plugins/my-connector-plugin/0df6d15c/0df6d15c.jar.sha512\n" +
                "\n" +
                "USER 1001\n\n"));
    }

    @ParallelTest
    public void testNoUrlWhenRequired() {

        Artifact tgzArtifact = new TgzArtifactBuilder()
                .build();
        Artifact jarArtifact = new JarArtifactBuilder()
                .build();
        Artifact zipArtifact = new ZipArtifactBuilder()
                .build();

        Build connectBuildTgz = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(singletonList(tgzArtifact))
                        .build())
                .build();

        Throwable e = assertThrows(InvalidConfigurationException.class, () -> new KafkaConnectDockerfile("myImage:latest", connectBuildTgz));

        assertThat(e.getMessage(), is("tgz artifact is missing an URL."));


        Build connectBuildZip = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(singletonList(zipArtifact))
                        .build())
                .build();

        e = assertThrows(InvalidConfigurationException.class, () -> new KafkaConnectDockerfile("myImage:latest", connectBuildZip));

        assertThat(e.getMessage(), is("zip artifact is missing an URL."));


        Build connectBuildJar = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(singletonList(jarArtifact))
                        .build())
                .build();

        e = assertThrows(InvalidConfigurationException.class, () -> new KafkaConnectDockerfile("myImage:latest", connectBuildJar));

        assertThat(e.getMessage(), is("jar artifact is missing an URL."));
    }

    @ParallelTest
    public void testMavenDockerfile()   {
        MavenArtifact mvn1 = new MavenArtifactBuilder()
                .withGroup("g1")
                .withArtifact("a1")
                .withVersion("v1")
                .build();

        MavenArtifact mvn2 = new MavenArtifactBuilder()
                .withGroup("g2")
                .withArtifact("a2")
                .withVersion("v2")
                .build();

        Build connectBuild = new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(mvn1, mvn2)
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
                "FROM registry.access.redhat.com/ubi8/openjdk-11:1.3-18 AS downloadArtifacts\n" +
                "RUN curl -L --create-dirs --output /tmp/401856c0/pom.xml https://repo1.maven.org/maven2/g1/a1/v1/a1-v1.pom \\\n" +
                "      && mvn dependency:copy-dependencies -DoutputDirectory=/tmp/artifacts/401856c0 -f /tmp/401856c0/pom.xml\n" +
                "\n" +
                "RUN curl -L --create-dirs --output /tmp/6ecfeffd/pom.xml https://repo1.maven.org/maven2/g2/a2/v2/a2-v2.pom \\\n" +
                "      && mvn dependency:copy-dependencies -DoutputDirectory=/tmp/artifacts/6ecfeffd -f /tmp/6ecfeffd/pom.xml\n" +
                "\n" +
                "FROM myImage:latest\n" +
                "\n" +
                "USER root:root\n" +
                "\n" +
                "##########\n" +
                "# Connector plugin my-connector-plugin\n" +
                "##########\n" +
                "RUN curl -L --create-dirs --output /opt/kafka/plugins/my-connector-plugin/401856c0/a1-v1.jar https://repo1.maven.org/maven2/g1/a1/v1/a1-v1.jar\n" +
                "COPY --from=downloadArtifacts /tmp/artifacts/401856c0 /opt/kafka/plugins/my-connector-plugin/401856c0\n" +
                "\n" +
                "RUN curl -L --create-dirs --output /opt/kafka/plugins/my-connector-plugin/6ecfeffd/a2-v2.jar https://repo1.maven.org/maven2/g2/a2/v2/a2-v2.jar\n" +
                "COPY --from=downloadArtifacts /tmp/artifacts/6ecfeffd /opt/kafka/plugins/my-connector-plugin/6ecfeffd\n" +
                "\n" +
                "USER 1001\n" +
                "\n"));
    }

    private Build getConnectBuildFromGAV(String g, String a, String v) {
        MavenArtifact mvn = new MavenArtifactBuilder()
                .withGroup(g)
                .withArtifact(a)
                .withVersion(v)
                .build();

        return new BuildBuilder()
                .withPlugins(new PluginBuilder()
                        .withName("my-connector-plugin")
                        .withArtifacts(mvn)
                        .build())
                .build();
    }

    @ParallelTest
    public void testForbiddenChars() {
        Build b1 = getConnectBuildFromGAV("$(echo rm -rf)", "a", "v");
        Build b2 = getConnectBuildFromGAV("g", "; print $PASSWORD", "v");
        Build b3 = getConnectBuildFromGAV("g", "a", "\\I AM VIRUS, PLEASE PAY ME");

        Throwable t1 = assertThrows(InvalidConfigurationException.class, () -> new KafkaConnectDockerfile("myImage:latest", b1));
        Throwable t2 = assertThrows(InvalidConfigurationException.class, () -> new KafkaConnectDockerfile("myImage:latest", b2));
        Throwable t3 = assertThrows(InvalidConfigurationException.class, () -> new KafkaConnectDockerfile("myImage:latest", b3));

        assertThat(t1.getMessage(), is("String '$(echo rm -rf)' contains forbidden character"));
        assertThat(t2.getMessage(), is("String '; print $PASSWORD' contains forbidden character"));
        assertThat(t3.getMessage(), is("String '\\I AM VIRUS, PLEASE PAY ME' contains forbidden character"));
    }
}
