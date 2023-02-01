/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorization;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorizationOpaBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static io.strimzi.operator.cluster.model.KafkaBrokerConfigurationBuilderTest.IsEquivalent.isEquivalent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ParallelSuite
public class KafkaBrokerConfigurationBuilderTest {
    @ParallelTest
    public void testBrokerId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId()
                .build();

        assertThat(configuration, isEquivalent("broker.id=${STRIMZI_BROKER_ID}\n" +
                "node.id=${STRIMZI_BROKER_ID}"));
    }

    @ParallelTest
    public void testPerBrokerBrokerId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .build();

        assertThat(configuration, isEquivalent("broker.id=2\n" +
                "node.id=2"));
    }

    @ParallelTest
    public void testKraft()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withKRaft("my-cluster", "my-namespace", 3)
                .build();

        assertThat(configuration, isEquivalent("process.roles=broker,controller\n" +
                "controller.listener.names=CONTROLPLANE-9090\n" +
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090"));
    }

    @ParallelTest
    public void testNoCruiseControl()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withCruiseControl("my-cluster", null, "1", "1", "1")
                .build();

        assertThat(configuration, isEquivalent(""));
    }

    @ParallelTest
    public void testCruiseControl()  {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder().build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withCruiseControl("my-cluster", cruiseControlSpec, "1", "1", "1")
                .build();

        assertThat(configuration, containsString(
                CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=strimzi.cruisecontrol.metrics\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO + "=HTTPS\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS + "=my-cluster-kafka-brokers:9091\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL + "=SSL\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE + "=PKCS12\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_LOCATION + "=/tmp/kafka/cluster.keystore.p12\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_PASSWORD + "=${CERTS_STORE_PASSWORD}\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE + "=PKCS12\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION + "=/tmp/kafka/cluster.truststore.p12\n" +
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD + "=${CERTS_STORE_PASSWORD}\n" +
                CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE + "=true\n" +
                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=1\n" +
                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=1\n" +
                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=1"));
    }

    @ParallelTest
    public void testCruiseControlCustomMetricReporterTopic()  {
        String metricReporterTopicName = "metric-reporter-topic";
        Map<String, Object> config = new HashMap<>();
        config.put(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), metricReporterTopicName);
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder().withConfig(config).build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withCruiseControl("my-cluster", cruiseControlSpec, "1", "1", "1")
                .build();

        assertThat(configuration, containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=" + metricReporterTopicName + "\n"));
    }

    @ParallelTest
    public void testNoRackAwareness()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withRackId(null)
                .build();

        assertThat(configuration, isEquivalent(""));
    }

    @ParallelTest
    public void testRackId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withRackId(new Rack("failure-domain.kubernetes.io/zone"))
                .build();

        assertThat(configuration, isEquivalent("broker.rack=${STRIMZI_RACK_ID}"));
    }

    @ParallelTest
    public void testRackAndBrokerId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId()
                .withRackId(new Rack("failure-domain.kubernetes.io/zone"))
                .build();

        assertThat(configuration, isEquivalent("broker.id=${STRIMZI_BROKER_ID}\n" +
                                                                "node.id=${STRIMZI_BROKER_ID}\n" +
                                                                "broker.rack=${STRIMZI_RACK_ID}"));
    }

    @ParallelTest
    public void testPerBrokerRackAndBrokerId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("0")
                .withRackId(new Rack("failure-domain.kubernetes.io/zone"))
                .build();

        assertThat(configuration, isEquivalent("broker.id=0\n" +
                "node.id=0\n" +
                "broker.rack=${STRIMZI_RACK_ID}"));
    }

    @ParallelTest
    public void testZookeeperConfig()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withZookeeper("my-cluster")
                .build();

        assertThat(configuration, isEquivalent(String.format("zookeeper.connect=%s:%d\n", KafkaResources.zookeeperServiceName("my-cluster"), ZookeeperCluster.CLIENT_TLS_PORT) +
                "zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty\n" +
                "zookeeper.ssl.client.enable=true\n" +
                "zookeeper.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12\n" +
                "zookeeper.ssl.keystore.password=${CERTS_STORE_PASSWORD}\n" +
                "zookeeper.ssl.keystore.type=PKCS12\n" +
                "zookeeper.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12\n" +
                "zookeeper.ssl.truststore.password=${CERTS_STORE_PASSWORD}\n" +
                "zookeeper.ssl.truststore.type=PKCS12"));
    }

    @ParallelTest
    public void testNoAuthorization()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", null, false)
                .build();

        assertThat(configuration, isEquivalent(""));
    }

    @ParallelTest
    public void testSimpleAuthorizationWithSuperUsers()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .addToSuperUsers("jakub", "CN=kuba")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, false)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=kafka.security.authorizer.AclAuthorizer\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jakub;User:CN=kuba"));
    }

    @ParallelTest
    public void testSimpleAuthorizationWithSuperUsersAndKRaft()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .addToSuperUsers("jakub", "CN=kuba")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, true)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer\n" +
                "early.start.listeners=CONTROLPLANE-9090,REPLICATION-9091\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jakub;User:CN=kuba"));
    }

    @ParallelTest
    public void testSimpleAuthorizationWithoutSuperUsers()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, false)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=kafka.security.authorizer.AclAuthorizer\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));
    }

    @ParallelTest
    public void testKeycloakAuthorization() {
        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName("my-secret")
                .withCertificate("my.crt")
                .build();

        KafkaAuthorization auth = new KafkaAuthorizationKeycloakBuilder()
                .withTokenEndpointUri("http://token-endpoint-uri")
                .withClientId("my-client-id")
                .withDelegateToKafkaAcls(false)
                .withGrantsRefreshPeriodSeconds(120)
                .withGrantsRefreshPoolSize(10)
                .withTlsTrustedCertificates(cert)
                .withDisableTlsHostnameVerification(true)
                .addToSuperUsers("giada", "CN=paccu")
                .withConnectTimeoutSeconds(30)
                .withEnableMetrics(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, false)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=io.strimzi.kafka.oauth.server.authorizer.KeycloakRBACAuthorizer\n" +
                "strimzi.authorization.token.endpoint.uri=http://token-endpoint-uri\n" +
                "strimzi.authorization.client.id=my-client-id\n" +
                "strimzi.authorization.delegate.to.kafka.acl=false\n" +
                "strimzi.authorization.kafka.cluster.name=my-cluster\n" +
                "strimzi.authorization.ssl.truststore.location=/tmp/kafka/authz-keycloak.truststore.p12\n" +
                "strimzi.authorization.ssl.truststore.password=${CERTS_STORE_PASSWORD}\n" +
                "strimzi.authorization.ssl.truststore.type=PKCS12\n" +
                "strimzi.authorization.ssl.endpoint.identification.algorithm=\n" +
                "strimzi.authorization.grants.refresh.period.seconds=120\n" +
                "strimzi.authorization.grants.refresh.pool.size=10\n" +
                "strimzi.authorization.connect.timeout.seconds=30\n" +
                "strimzi.authorization.enable.metrics=true\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:giada;User:CN=paccu"));
    }

    @ParallelTest
    public void testKeycloakAuthorizationWithDefaults() {
        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName("my-secret")
                .withCertificate("my.crt")
                .build();

        KafkaAuthorization auth = new KafkaAuthorizationKeycloakBuilder()
                .withTokenEndpointUri("http://token-endpoint-uri")
                .withClientId("my-client-id")
                .withTlsTrustedCertificates(cert)
                .withReadTimeoutSeconds(30)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, false)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=io.strimzi.kafka.oauth.server.authorizer.KeycloakRBACAuthorizer\n" +
                "strimzi.authorization.token.endpoint.uri=http://token-endpoint-uri\n" +
                "strimzi.authorization.client.id=my-client-id\n" +
                "strimzi.authorization.delegate.to.kafka.acl=false\n" +
                "strimzi.authorization.kafka.cluster.name=my-cluster\n" +
                "strimzi.authorization.ssl.truststore.location=/tmp/kafka/authz-keycloak.truststore.p12\n" +
                "strimzi.authorization.ssl.truststore.password=${CERTS_STORE_PASSWORD}\n" +
                "strimzi.authorization.ssl.truststore.type=PKCS12\n" +
                "strimzi.authorization.ssl.endpoint.identification.algorithm=HTTPS\n" +
                "strimzi.authorization.read.timeout.seconds=30\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));
    }

    @ParallelTest
    public void testOpaAuthorizationWithDefaults() {
        KafkaAuthorization auth = new KafkaAuthorizationOpaBuilder()
                .withUrl("http://opa:8181/v1/data/kafka/allow")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, false)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer\n" +
                "opa.authorizer.url=http://opa:8181/v1/data/kafka/allow\n" +
                "opa.authorizer.allow.on.error=false\n" +
                "opa.authorizer.metrics.enabled=false\n" +
                "opa.authorizer.cache.initial.capacity=5000\n" +
                "opa.authorizer.cache.maximum.size=50000\n" +
                "opa.authorizer.cache.expire.after.seconds=3600\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));
    }

    @ParallelTest
    public void testOpaAuthorization() {
        KafkaAuthorization auth = new KafkaAuthorizationOpaBuilder()
                .withUrl("http://opa:8181/v1/data/kafka/allow")
                .withAllowOnError(true)
                .withInitialCacheCapacity(1000)
                .withMaximumCacheSize(10000)
                .withExpireAfterMs(60000)
                .addToSuperUsers("jack", "CN=conor")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withAuthorization("my-cluster", auth, false)
                .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer\n" +
                "opa.authorizer.url=http://opa:8181/v1/data/kafka/allow\n" +
                "opa.authorizer.allow.on.error=true\n" +
                "opa.authorizer.metrics.enabled=false\n" +
                "opa.authorizer.cache.initial.capacity=1000\n" +
                "opa.authorizer.cache.maximum.size=10000\n" +
                "opa.authorizer.cache.expire.after.seconds=60\n" +
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jack;User:CN=conor"));
    }

    @ParallelTest
    public void testOpaAuthorizationWithTls() {
        CertSecretSource cert = new CertSecretSourceBuilder()
            .withSecretName("my-secret")
            .withCertificate("my.crt")
            .build();

        KafkaAuthorization auth = new KafkaAuthorizationOpaBuilder()
            .withUrl("https://opa:8181/v1/data/kafka/allow")
            .withAllowOnError(true)
            .withInitialCacheCapacity(1000)
            .withMaximumCacheSize(10000)
            .withExpireAfterMs(60000)
            .withTlsTrustedCertificates(cert)
            .addToSuperUsers("jack", "CN=conor")
            .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
            .withAuthorization("my-cluster", auth, false)
            .build();

        assertThat(configuration, isEquivalent("authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer\n" +
            "opa.authorizer.url=https://opa:8181/v1/data/kafka/allow\n" +
            "opa.authorizer.allow.on.error=true\n" +
            "opa.authorizer.metrics.enabled=false\n" +
            "opa.authorizer.cache.initial.capacity=1000\n" +
            "opa.authorizer.cache.maximum.size=10000\n" +
            "opa.authorizer.cache.expire.after.seconds=60\n" +
            "opa.authorizer.truststore.path=/tmp/kafka/authz-opa.truststore.p12\n" +
            "opa.authorizer.truststore.password=${CERTS_STORE_PASSWORD}\n" +
            "opa.authorizer.truststore.type=PKCS12\n" +
            "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jack;User:CN=conor"));
    }

    @ParallelTest
    public void testNullUserConfiguration()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withUserConfiguration(null)
                .build();

        assertThat(configuration, isEquivalent(""));
    }

    @ParallelTest
    public void testEmptyUserConfiguration()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withUserConfiguration(kafkaConfiguration)
                .build();

        assertThat(configuration, isEquivalent(""));
    }

    @ParallelTest
    public void testUserConfiguration()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("auto.create.topics.enable", "false");
        userConfiguration.put("offsets.topic.replication.factor", 3);
        userConfiguration.put("transaction.state.log.replication.factor", 3);
        userConfiguration.put("transaction.state.log.min.isr", 2);

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withUserConfiguration(kafkaConfiguration)
                .build();

        assertThat(configuration, isEquivalent("auto.create.topics.enable=false\n" +
                                                            "offsets.topic.replication.factor=3\n" +
                                                            "transaction.state.log.replication.factor=3\n" +
                                                            "transaction.state.log.min.isr=2"));
    }

    @ParallelTest
    public void testEphemeralStorageLogDirs()  {
        Storage storage = new EphemeralStorageBuilder()
                .withSizeLimit("5Gi")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, "/var/lib/kafka", false))
                .build();

        assertThat(configuration, isEquivalent("log.dirs=/var/lib/kafka/data/kafka-log${STRIMZI_BROKER_ID}"));
    }

    @ParallelTest
    public void testPerBrokerEphemeralStorageLogDirs()  {
        Storage storage = new EphemeralStorageBuilder()
                .withSizeLimit("5Gi")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, "/var/lib/kafka", false))
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "log.dirs=/var/lib/kafka/data/kafka-log2"));
    }

    @ParallelTest
    public void testPersistentStorageLogDirs()  {
        Storage storage = new PersistentClaimStorageBuilder()
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, "/var/lib/kafka", false))
                .build();

        assertThat(configuration, isEquivalent("log.dirs=/var/lib/kafka/data/kafka-log${STRIMZI_BROKER_ID}"));
    }

    @ParallelTest
    public void testPerBrokerPersistentStorageLogDirs()  {
        Storage storage = new PersistentClaimStorageBuilder()
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, "/var/lib/kafka", false))
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "log.dirs=/var/lib/kafka/data/kafka-log2"));
    }

    @ParallelTest
    public void testJbodStorageLogDirs()  {
        SingleVolumeStorage vol1 = new PersistentClaimStorageBuilder()
                .withId(1)
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        SingleVolumeStorage vol2 = new EphemeralStorageBuilder()
                .withId(2)
                .withSizeLimit("5Gi")
                .build();

        SingleVolumeStorage vol5 = new PersistentClaimStorageBuilder()
                .withId(5)
                .withSize("10Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(false)
                .build();

        Storage storage = new JbodStorageBuilder()
                .withVolumes(vol1, vol2, vol5)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, "/var/lib/kafka", false))
                .build();

        assertThat(configuration, isEquivalent("log.dirs=/var/lib/kafka/data-1/kafka-log${STRIMZI_BROKER_ID},/var/lib/kafka/data-2/kafka-log${STRIMZI_BROKER_ID},/var/lib/kafka/data-5/kafka-log${STRIMZI_BROKER_ID}"));
    }

    @ParallelTest
    public void testPerBrokerJbodStorageLogDirs()  {
        SingleVolumeStorage vol1 = new PersistentClaimStorageBuilder()
                .withId(1)
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        SingleVolumeStorage vol2 = new EphemeralStorageBuilder()
                .withId(2)
                .withSizeLimit("5Gi")
                .build();

        SingleVolumeStorage vol5 = new PersistentClaimStorageBuilder()
                .withId(5)
                .withSize("10Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(false)
                .build();

        Storage storage = new JbodStorageBuilder()
                .withVolumes(vol1, vol2, vol5)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, "/var/lib/kafka", false))
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "log.dirs=/var/lib/kafka/data-1/kafka-log2,/var/lib/kafka/data-2/kafka-log2,/var/lib/kafka/data-5/kafka-log2"));
    }

    @ParallelTest
    public void testWithNoListeners() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", emptyList())
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testPerBrokerWithNoListeners() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withListeners("my-cluster", "my-namespace", emptyList())
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testConnectionLimits()  {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9100)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withMaxConnections(100)
                    .withMaxConnectionCreationRate(10)
                .endConfiguration()
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9101)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withMaxConnections(1000)
                    .withMaxConnectionCreationRate(50)
                .endConfiguration()
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener3")
                .withPort(9102)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                .endConfiguration()
                .build();

        GenericKafkaListener listener4 = new GenericKafkaListenerBuilder()
                .withName("listener4")
                .withPort(9103)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", asList(listener1, listener2, listener3, listener4))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.listener1-9100.max.connections=100",
                "listener.name.listener1-9100.max.connection.creation.rate=10",
                "listener.name.listener2-9101.max.connections=1000",
                "listener.name.listener2-9101.max.connection.creation.rate=50",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,LISTENER1-9100://0.0.0.0:9100,LISTENER2-9101://0.0.0.0:9101,LISTENER3-9102://0.0.0.0:9102,LISTENER4-9103://0.0.0.0:9103",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,LISTENER1-9100://${STRIMZI_LISTENER1_9100_ADVERTISED_HOSTNAME}:${STRIMZI_LISTENER1_9100_ADVERTISED_PORT},LISTENER2-9101://${STRIMZI_LISTENER2_9101_ADVERTISED_HOSTNAME}:${STRIMZI_LISTENER2_9101_ADVERTISED_PORT},LISTENER3-9102://${STRIMZI_LISTENER3_9102_ADVERTISED_HOSTNAME}:${STRIMZI_LISTENER3_9102_ADVERTISED_PORT},LISTENER4-9103://${STRIMZI_LISTENER4_9103_ADVERTISED_HOSTNAME}:${STRIMZI_LISTENER4_9103_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,LISTENER1-9100:PLAINTEXT,LISTENER2-9101:PLAINTEXT,LISTENER3-9102:PLAINTEXT,LISTENER4-9103:PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithPlainListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testPerBrokerWithPlainListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withListeners("my-cluster", "my-namespace", singletonList(listener), () -> "my-cluster-kafka-2", listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092", false)
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testKraftListeners()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withKRaft("my-cluster", "my-namespace", 3)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), () -> "my-cluster-kafka-2", listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092", true)
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithPlainListenersWithSaslAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationScramSha512Auth()
                .endKafkaListenerAuthenticationScramSha512Auth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;",
                "listener.name.plain-9092.sasl.enabled.mechanisms=SCRAM-SHA-512"));
    }

    @ParallelTest
    public void testWithTlsListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://${STRIMZI_TLS_9093_ADVERTISED_HOSTNAME}:${STRIMZI_TLS_9093_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.tls-9093.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithTlsListenersWithTlsAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://${STRIMZI_TLS_9093_ADVERTISED_HOSTNAME}:${STRIMZI_TLS_9093_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.client.auth=required",
                "listener.name.tls-9093.ssl.truststore.location=/tmp/kafka/clients.truststore.p12",
                "listener.name.tls-9093.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.truststore.type=PKCS12",
                "listener.name.tls-9093.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.tls-9093.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithTlsListenersWithCustomCerts()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("my-secret")
                        .withKey("my.key")
                        .withCertificate("my.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://${STRIMZI_TLS_9093_ADVERTISED_HOSTNAME}:${STRIMZI_TLS_9093_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.keystore.location=/tmp/kafka/custom-tls-9093.keystore.p12",
                "listener.name.tls-9093.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithTlsAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.client.auth=required",
                "listener.name.external-9094.ssl.truststore.location=/tmp/kafka/clients.truststore.p12",
                "listener.name.external-9094.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.truststore.type=PKCS12",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithSaslAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewKafkaListenerAuthenticationScramSha512Auth()
                .endKafkaListenerAuthenticationScramSha512Auth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SASL_SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;",
                "listener.name.external-9094.sasl.enabled.mechanisms=SCRAM-SHA-512",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithCustomCerts()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("my-secret")
                        .withKey("my.key")
                        .withCertificate("my.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/custom-external-9094.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersLoadBalancerWithTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .build();
        
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testPerBrokerWithExternalListeners()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withListeners("my-cluster", "my-namespace", singletonList(listener), () -> "my-cluster-kafka-2", listenerId -> "my-lb.com", listenerId -> "9094", false)
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://my-lb.com:9094",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersLoadBalancerWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithExternalListenersNodePortWithTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersNodePortWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testPerBrokerWithExternalListenersNodePortWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withBrokerId("2")
                .withListeners("my-cluster", "my-namespace", singletonList(listener), () -> "my-cluster-kafka-2", listenerId -> "${STRIMZI_NODEPORT_DEFAULT_ADDRESS}", listenerId -> "31234", false)
                .build();

        assertThat(configuration, isEquivalent("broker.id=2",
                "node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_NODEPORT_DEFAULT_ADDRESS}:31234",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithExternalListenersIngress()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withHost("broker-0.mytld.com")
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .withNewConfiguration()
                    .withControllerClass("nginx-ingress")
                    .withNewBootstrap()
                        .withHost("bootstrap.mytld.com")
                    .endBootstrap()
                    .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersClusterIPWithTLS()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("ingress.mytld.com")
                .withAdvertisedPort(12345)
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(true)
                .withNewConfiguration()
                .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersClusterIPWithoutTLS()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("ingress.mytld.com")
                .withAdvertisedPort(12345)
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(false)
                .withNewConfiguration()
                .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testOauthConfiguration()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks")
                    .withEnableECDSA(true)
                    .withUserNameClaim("preferred_username")
                    .withGroupsClaim("$.groups")
                    .withGroupsClaimDelimiter(";")
                    .withMaxSecondsWithoutReauthentication(3600)
                    .withJwksMinRefreshPauseSeconds(5)
                    .withEnablePlain(true)
                    .withTokenEndpointUri("http://token")
                    .withConnectTimeoutSeconds(30)
                    .withReadTimeoutSeconds(30)
                    .withEnableMetrics(true)
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.config.id=\"PLAIN-9092\";",
                "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER,PLAIN",
                "listener.name.plain-9092.connections.max.reauth.ms=3600000"));
    }

    @ParallelTest
    public void testOauthConfigurationWithPlainOnly()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                .withValidIssuerUri("http://valid-issuer")
                .withJwksEndpointUri("http://jwks")
                .withUserNameClaim("preferred_username")
                .withMaxSecondsWithoutReauthentication(3600)
                .withJwksMinRefreshPauseSeconds(5)
                .withEnablePlain(true)
                .withEnableOauthBearer(false)
                .withTokenEndpointUri("http://token")
                .withClientAudience("kafka")
                .withClientScope("messaging")
                .withConnectTimeoutSeconds(30)
                .withEnableMetrics(true)
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.scope=\"messaging\" oauth.audience=\"kafka\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.connect.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=PLAIN",
                "listener.name.plain-9092.connections.max.reauth.ms=3600000"));
    }

    @ParallelTest
    public void testOauthConfigurationWithoutOptions()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.config.id=\"PLAIN-9092\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));
    }

    @ParallelTest
    public void testOauthConfigurationWithTlsConfig()  {
        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName("my-secret")
                .withCertificate("my.crt")
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                    .withValidIssuerUri("https://valid-issuer")
                    .withJwksEndpointUri("https://jwks")
                    .withEnableECDSA(true)
                    .withUserNameClaim("preferred_username")
                    .withDisableTlsHostnameVerification(true)
                    .withTlsTrustedCertificates(cert)
                .endKafkaListenerAuthenticationOAuth()
                .build();
        
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"https://valid-issuer\" oauth.jwks.endpoint.uri=\"https://jwks\" oauth.username.claim=\"preferred_username\" oauth.ssl.endpoint.identification.algorithm=\"\" oauth.config.id=\"PLAIN-9092\" oauth.ssl.truststore.location=\"/tmp/kafka/oauth-plain-9092.truststore.p12\" oauth.ssl.truststore.password=\"${CERTS_STORE_PASSWORD}\" oauth.ssl.truststore.type=\"PKCS12\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));
    }

    @ParallelTest
    public void testOauthConfigurationWithClientSecret()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                    .withValidIssuerUri("https://valid-issuer")
                    .withIntrospectionEndpointUri("https://intro")
                    .withCheckAudience(true)
                    .withCustomClaimCheck("'kafka-user' in @.roles.client-roles.kafka")
                    .withClientId("my-oauth-client")
                    .withNewClientSecret()
                        .withSecretName("my-secret")
                        .withKey("client-secret")
                    .endClientSecret()
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://${STRIMZI_PLAIN_9092_ADVERTISED_HOSTNAME}:${STRIMZI_PLAIN_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.client.id=\"my-oauth-client\" oauth.valid.issuer.uri=\"https://valid-issuer\" oauth.check.audience=\"true\" oauth.custom.claim.check=\"'kafka-user' in @.roles.client-roles.kafka\" oauth.introspection.endpoint.uri=\"https://intro\" oauth.config.id=\"PLAIN-9092\" oauth.client.secret=\"${STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET}\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));
    }

    @ParallelTest
    public void testOAuthOptions()  {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .withValidIssuerUri("http://valid-issuer")
                .withCheckIssuer(false)
                .withCheckAudience(true)
                .withJwksEndpointUri("http://jwks-endpoint")
                .withIntrospectionEndpointUri("http://introspection-endpoint")
                .withUserInfoEndpointUri("http://userinfo-endpoint")
                .withJwksExpirySeconds(160)
                .withJwksRefreshSeconds(50)
                .withJwksMinRefreshPauseSeconds(5)
                .withJwksIgnoreKeyUse()
                .withEnableECDSA(true)
                .withUserNameClaim("preferred_username")
                .withFallbackUserNameClaim("client_id")
                .withFallbackUserNamePrefix("client-account-")
                .withCheckAccessTokenType(false)
                .withClientId("my-kafka-id")
                .withAccessTokenIsJwt(false)
                .withValidTokenType("access_token")
                .withDisableTlsHostnameVerification(true)
                .withMaxSecondsWithoutReauthentication(3600)
                .withEnablePlain(true)
                .withTokenEndpointUri("http://token")
                .withCustomClaimCheck("@.aud && @.aud == 'something'")
                .withConnectTimeoutSeconds(30)
                .withReadTimeoutSeconds(60)
                .withClientAudience("kafka")
                .withClientScope("messaging")
                .withEnableMetrics(true)
                .withFailFast(false)
                .build();

        List<String> expectedOptions = new ArrayList<>(5);
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CLIENT_ID, "my-kafka-id"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CHECK_ISSUER, false));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CHECK_AUDIENCE, true));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CUSTOM_CLAIM_CHECK, "@.aud && @.aud == 'something'"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_SCOPE, "messaging"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_AUDIENCE, "kafka"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_JWKS_ENDPOINT_URI, "http://jwks-endpoint"));
        expectedOptions.add(String.format("%s=\"%d\"", ServerConfig.OAUTH_JWKS_REFRESH_SECONDS, 50));
        expectedOptions.add(String.format("%s=\"%d\"", ServerConfig.OAUTH_JWKS_EXPIRY_SECONDS, 160));
        expectedOptions.add(String.format("%s=\"%d\"", ServerConfig.OAUTH_JWKS_REFRESH_MIN_PAUSE_SECONDS, 5));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_JWKS_IGNORE_KEY_USE, true));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection-endpoint"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_USERINFO_ENDPOINT_URI, "http://userinfo-endpoint"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_USERNAME_CLAIM, "preferred_username"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_FALLBACK_USERNAME_CLAIM, "client_id"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_FALLBACK_USERNAME_PREFIX, "client-account-"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_ACCESS_TOKEN_IS_JWT, false));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CHECK_ACCESS_TOKEN_TYPE, false));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_VALID_TOKEN_TYPE, "access_token"));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, 30));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_READ_TIMEOUT_SECONDS, 60));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_ENABLE_METRICS, true));
        expectedOptions.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_FAIL_FAST, false));

        // enablePlain and tokenEndpointUri are handled separately from getOAuthOptions
        List<String> actualOptions = KafkaBrokerConfigurationBuilder.getOAuthOptions(auth);

        assertThat(actualOptions, is(equalTo(expectedOptions)));
    }

    @ParallelTest
    public void testCustomAuthConfigSetProtocolMapCorrectlyForsSslSasl() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, configuration.contains("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_SSL"));
    }

    @ParallelTest
    public void testCustomAuthConfigSetProtocolMapCorrectlyForPlainSasl() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, configuration.contains("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_PLAINTEXT"));
    }


    @ParallelTest
    public void testCustomAuthConfigSetProtocolMapCorrectlyForPlain() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(false)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, configuration.contains("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:PLAINTEXT"));
    }

    @ParallelTest
    public void testCustomAuthConfigRemovesForbiddenPrefixes() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(false)
                .withListenerConfig(Map.of("ssl.truststore.path", "foo"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, !configuration.contains("ssl.truststore.path"));
    }

    @ParallelTest
    public void testCustomAuthConfigPrefixesUserProvidedConfig() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of("oauthbearer.sasl.client.callback.handler.class", "client.class",
                        "oauthbearer.sasl.server.callback.handler.class", "server.class",
                        "oauthbearer.sasl.login.callback.handler.class", "login.class",
                        "oauthbearer.connections.max.reauth.ms", 999999999,
                        "sasl.enabled.mechanisms", "oauthbearer",
                        "oauthbearer.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION)
                .withListeners("my-cluster", "my-namespace", singletonList(listener))
                .build();

        assertThat(configuration, isEquivalent("listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.custom-listener-9092.ssl.keystore.password=${CERTS_STORE_PASSWORD}",
                "listener.name.custom-listener-9092.ssl.keystore.type=PKCS12",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,CUSTOM-LISTENER-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster" +
                        "-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-${STRIMZI_BROKER_ID}.my-cluster-kafka-brokers.my-namespace.svc:9091,CUSTOM-LISTENER-9092://${STRIMZI_CUSTOM-LISTENER_9092_ADVERTISED_HOSTNAME}:${STRIMZI_CUSTOM-LISTENER_9092_ADVERTISED_PORT}",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_SSL",
                "control.plane.listener.name=CONTROLPLANE-9090",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.custom-listener-9092.sasl.enabled.mechanisms=oauthbearer",
                "listener.name.custom-listener-9092.oauthbearer.sasl.client.callback.handler.class=client.class",
                "listener.name.custom-listener-9092.oauthbearer.sasl.server.callback.handler.class=server.class",
                "listener.name.custom-listener-9092.oauthbearer.sasl.login.callback.handler.class=login.class",
                "listener.name.custom-listener-9092.oauthbearer.connections.max.reauth.ms=999999999",
                "listener.name.custom-listener-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"));
    }

    @ParallelTest
    public void testOAuthDefaultOptions()  {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .build();

        List<String> actualOptions = KafkaBrokerConfigurationBuilder.getOAuthOptions(auth);

        assertThat(actualOptions, is(equalTo(Collections.emptyList())));
    }

    static class IsEquivalent extends TypeSafeMatcher<String> {
        private final List<String> expectedLines;

        public IsEquivalent(String expectedConfig) {
            super();
            this.expectedLines = ModelUtils.getLinesWithoutCommentsAndEmptyLines(expectedConfig);
        }

        public IsEquivalent(List<String> expectedLines) {
            super();
            this.expectedLines = expectedLines;
        }

        @Override
        protected boolean matchesSafely(String config) {
            List<String> actualLines = ModelUtils.getLinesWithoutCommentsAndEmptyLines(config);

            return expectedLines.containsAll(actualLines) && actualLines.containsAll(expectedLines);
        }

        private String getLinesAsString(Collection<String> configLines)   {
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);

            for (String line : configLines) {
                writer.println(line);
            }

            return stringWriter.toString();
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(getLinesAsString(new TreeSet<>(expectedLines)));
        }

        @Override
        protected void describeMismatchSafely(String item, Description mismatchDescription) {
            mismatchDescription.appendText(" was: \n")
                    .appendText(getLinesAsString(new TreeSet<>(ModelUtils.getLinesWithoutCommentsAndEmptyLines(item))))
                    .appendText("\n\nOriginal value: \n")
                    .appendText(item);
        }

        public static Matcher<String> isEquivalent(String expectedConfig) {
            return new IsEquivalent(expectedConfig);
        }

        public static Matcher<String> isEquivalent(String... expectedLines) {
            return new IsEquivalent(asList(expectedLines));
        }
    }
}
