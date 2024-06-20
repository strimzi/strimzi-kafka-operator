/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsers;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsersBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.Util.decodeFromBase64;
import static io.strimzi.operator.common.Util.encodeToBase64;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.AUTH_FILE_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.HEALTHCHECK_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.REBALANCE_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ApiCredentialTest {
    private final static String CLUSTER = "my-cluster";
    private final static String NAMESPACE = "my-namespace";
    private final static int REPLICAS = 3;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String SECRET_NAME = "secretName";
    private static final String SECRET_KEY = "secretKey";

    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-name")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type");

    private static final HashLoginServiceApiUsers API_USERS = new HashLoginServiceApiUsersBuilder()
        .withNewValueFrom()
            .withNewSecretKeyRef(SECRET_KEY, SECRET_NAME, false)
        .endValueFrom()
        .build();

    private final static Set<NodeRef> NODES = Set.of(new NodeRef("foo-kafka-0", 0, "kafka", false, true));
    private final static Map<String, Storage> STORAGE = Map.of("kafka", new EphemeralStorageBuilder().withSizeLimit(("10Gi")).build());

    private static Kafka createKafka(HashLoginServiceApiUsers apiUsers) {
        return new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewZookeeper()
                    .withReplicas(REPLICAS)
                .endZookeeper()
                .withNewKafka()
                    .withReplicas(REPLICAS)
                    .withNewEphemeralStorage()
                        .withSizeLimit("10Gi")
                    .endEphemeralStorage()
                .endKafka()
                .withNewCruiseControl()
                    .withApiUsers(apiUsers)
                .endCruiseControl()
            .endSpec()
            .build();
    }

    private static Secret createSecret(Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(SECRET_NAME)
                .endMetadata()
                .addToData(data)
                .build();
    }

    @ParallelTest
    public void testApiUsersObjectCreation() {
        CruiseControlSpec s1 = new CruiseControlSpecBuilder()
            .withNewApiUsers()
                .withNewValueFrom()
                    .withNewSecretKeyRef(SECRET_KEY, SECRET_NAME, false)
                .endValueFrom()
            .endApiUsers()
            .build();
        ApiCredentials apiCredentials = new ApiCredentials(NAMESPACE, CLUSTER, LABELS, OWNER_REFERENCE, s1);
        assertThat(apiCredentials.getUserManagedApiSecretName(), is(SECRET_NAME));
        assertThat(apiCredentials.getUserManagedApiSecretKey(), is(SECRET_KEY));

        // Ensure exception is thrown when invalid apiUsers config is provided
        CruiseControlSpec s2 = new CruiseControlSpecBuilder()
            .withNewApiUsers()
                    .withNewValueFrom()
                        .withNewSecretKeyRef(null, SECRET_NAME, false)
                    .endValueFrom()
                    .endApiUsers()
                    .build();
        assertThrows(InvalidResourceException.class, () -> new ApiCredentials(NAMESPACE, CLUSTER, LABELS, OWNER_REFERENCE, s2));

        // Ensure exception is thrown when invalid apiUsers config is provided
        CruiseControlSpec s3 = new CruiseControlSpecBuilder()
            .withNewApiUsers()
                .withNewValueFrom()
                .endValueFrom()
            .endApiUsers()
            .build();
        assertThrows(InvalidResourceException.class, () -> new ApiCredentials(NAMESPACE, CLUSTER, LABELS, OWNER_REFERENCE, s3));

        // Ensure exception is thrown when invalid apiUsers config is provided
        CruiseControlSpec s4 = new CruiseControlSpecBuilder()
            .withNewApiUsers()
            .endApiUsers()
            .build();
        assertThrows(InvalidResourceException.class, () -> new ApiCredentials(NAMESPACE, CLUSTER, LABELS, OWNER_REFERENCE, s4));
    }

    private void assertParseThrows(String illegalConfig) {
        assertThrows(InvalidConfigurationException.class, () -> ApiCredentials.parseEntriesFromString(illegalConfig));
    }

    @ParallelTest
    public void testParseEntriesFromString() {
        String config = """
                        username0: password0,USER
                        username1: password1,VIEWER
                        username2: password2,USER
                        """;
        Map<String, ApiCredentials.UserEntry> entries =  ApiCredentials.parseEntriesFromString(config);
        assertThat(entries.get("username0").username(), is("username0"));
        assertThat(entries.get("username0").password(), is("password0"));
        assertThat(entries.get("username0").role(), is(ApiCredentials.Role.USER));

        assertParseThrows("""
                            username1: , USER
                            """);
        assertParseThrows("""
                             : password1, USER
                            """);
        assertParseThrows("""
                            username1 username2: password1 USER
                            """);
        assertParseThrows("""
                            username1: password1,USER,VIEWER
                            """);
        assertParseThrows("""
                            username1: password1 password2,USER
                            """);
    }

    @ParallelTest
    public void testGenerateToManagedApiCredentials() {
        Secret secret = createSecret(Map.of("topic-operator.apiAdminName",  encodeToBase64("topic-operator"),
                "topic-operator.apiAdminPassword",  encodeToBase64("password")));
        Map<String, ApiCredentials.UserEntry> entries =  ApiCredentials.generateToManagedApiCredentials(secret);
        assertThat(entries.get("topic-operator").username(), is("topic-operator"));
        assertThat(entries.get("topic-operator").password(), is("password"));
        assertThat(entries.get("topic-operator").role(), is(ApiCredentials.Role.ADMIN));

        entries =  ApiCredentials.generateToManagedApiCredentials(null);
        assertThat(entries.size(), is(0));
    }

    private void assertParseThrowsForUserManagedCreds(String illegalConfig) {
        illegalConfig = encodeToBase64(illegalConfig);
        Secret secret = createSecret(Map.of(SECRET_KEY, illegalConfig));
        assertThrows(InvalidConfigurationException.class, () -> ApiCredentials.generateUserManagedApiCredentials(secret, SECRET_KEY));
    }

    @ParallelTest
    public void testGenerateUserManagedApiCredentials() {
        String config = encodeToBase64("""
                        username0: password0,USER
                        username1: password1,VIEWER
                        username2: password2,USER
                        """);
        Secret secret = createSecret(Map.of(SECRET_KEY, config));
        Map<String, ApiCredentials.UserEntry> entries =  ApiCredentials.generateUserManagedApiCredentials(secret, SECRET_KEY);

        assertThat(entries.get("username0").username(), is("username0"));
        assertThat(entries.get("username0").password(), is("password0"));
        assertThat(entries.get("username0").role(), is(ApiCredentials.Role.USER));

        assertThat(entries.get("username1").username(), is("username1"));
        assertThat(entries.get("username1").password(), is("password1"));
        assertThat(entries.get("username1").role(), is(ApiCredentials.Role.VIEWER));

        assertThat(entries.get("username2").username(), is("username2"));
        assertThat(entries.get("username2").password(), is("password2"));
        assertThat(entries.get("username2").role(), is(ApiCredentials.Role.USER));

        assertThat(ApiCredentials.generateUserManagedApiCredentials(secret, null), is(Collections.emptyMap()));
        assertThat(ApiCredentials.generateUserManagedApiCredentials(secret, "key"), is(Collections.emptyMap()));
        assertThat(ApiCredentials.generateUserManagedApiCredentials(null, SECRET_KEY), is(Collections.emptyMap()));

        assertParseThrowsForUserManagedCreds("""
                            username1: password1,USER
                            username2: password2,TEST
                            """);
        assertParseThrowsForUserManagedCreds("""
                            rebalance-operator: password1,USER
                            username2: password2,USER
                            """);
        assertParseThrowsForUserManagedCreds("""
                            topic-operator: password1,USER
                            username2: password2,USER
                            """);
        assertParseThrowsForUserManagedCreds("""
                            healthcheck: password1,USER
                            username2: password2,USER
                            """);
        assertParseThrowsForUserManagedCreds("""
                            username1: password1,USER
                            username2: password2,ADMIN
                            """);
        assertParseThrowsForUserManagedCreds("""
                            username1: password1,USER
                            username1: password2,ADMIN
                            """);
    }

    @ParallelTest
    public void testGenerateCoManagedApiCredentials() {
        PasswordGenerator mockPasswordGenerator = new PasswordGenerator(10, "a", "a");

        // Test that credentials from previous secret are reused
        Map<String, String> map1 = Map.of("cruise-control.authFile",
                encodeToBase64("rebalance-operator: password,ADMIN\n" +
                                     "healthcheck: password,USER"));
        Map<String, ApiCredentials.UserEntry> entries =  ApiCredentials.generateCoManagedApiCredentials(mockPasswordGenerator, createSecret(map1));
        assertThat(entries.get("rebalance-operator").username(), is("rebalance-operator"));
        assertThat(entries.get("rebalance-operator").password(), is("password"));
        assertThat(entries.get("rebalance-operator").role(), is(ApiCredentials.Role.ADMIN));

        assertThat(entries.get("healthcheck").username(), is("healthcheck"));
        assertThat(entries.get("healthcheck").password(), is("password"));
        assertThat(entries.get("healthcheck").role(), is(ApiCredentials.Role.USER));

        // Test malformed secret credentials with blank password for user throws error
        final Map<String, String> map2 = Map.of("cruise-control.authFile",
                encodeToBase64("rebalance-operator: ,ADMIN\n" +
                                     "healthcheck: password,USER"));
        assertThrows(InvalidConfigurationException.class, () -> ApiCredentials.generateCoManagedApiCredentials(mockPasswordGenerator, createSecret(map2)));

        // Test that new credentials are generated if they do not exist already
        entries = ApiCredentials.generateCoManagedApiCredentials(mockPasswordGenerator, null);
        assertThat(entries.get("rebalance-operator").username(), is(REBALANCE_OPERATOR_USERNAME));
        assertThat(entries.get("rebalance-operator").password(), is(not(emptyString())));
        assertThat(entries.get("rebalance-operator").role(), is(ApiCredentials.Role.ADMIN));

        assertThat(entries.get("healthcheck").username(), is(HEALTHCHECK_USERNAME));
        assertThat(entries.get("healthcheck").password(), is(not(emptyString())));
        assertThat(entries.get("healthcheck").role(), is(ApiCredentials.Role.USER));
    }

    @ParallelTest
    public void testGenerateApiSecret() {
        Secret oldCruiseControlApiSecret = createSecret(Map.of(AUTH_FILE_KEY,
                encodeToBase64("rebalance-operator: password,ADMIN\n" +
                                     "healthcheck: password,USER")));
        Secret userManagedApiSecret = createSecret(Map.of(SECRET_KEY,
                encodeToBase64("""
                        username0: password0,USER
                        username1: password1,VIEWER
                        """)));
        Secret topicOperatorManagedApiSecret = createSecret(
                Map.of(TOPIC_OPERATOR_USERNAME_KEY,  encodeToBase64("topic-operator"),
                       TOPIC_OPERATOR_PASSWORD_KEY,  encodeToBase64("password")));

        CruiseControl cc1 = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, createKafka(API_USERS), VERSIONS, NODES, STORAGE, Map.of(), SHARED_ENV_PROVIDER);
        Secret newCcApiUsersSecret = cc1.apiCredentials().generateApiSecret(
                new PasswordGenerator(10, "a", "a"),
                oldCruiseControlApiSecret,
                userManagedApiSecret,
                topicOperatorManagedApiSecret);

        Map<String, ApiCredentials.UserEntry> userEntries =  ApiCredentials.parseEntriesFromString(decodeFromBase64(newCcApiUsersSecret.getData().get(AUTH_FILE_KEY)));
        assertThat(userEntries.get("rebalance-operator").username(), is(REBALANCE_OPERATOR_USERNAME));
        assertThat(userEntries.get("rebalance-operator").password(), is("password"));
        assertThat(userEntries.get("rebalance-operator").role(), is(ApiCredentials.Role.ADMIN));

        assertThat(userEntries.get("healthcheck").username(), is(HEALTHCHECK_USERNAME));
        assertThat(userEntries.get("healthcheck").password(), is("password"));
        assertThat(userEntries.get("healthcheck").role(), is(ApiCredentials.Role.USER));

        assertThat(userEntries.get("topic-operator").username(), is("topic-operator"));
        assertThat(userEntries.get("topic-operator").password(), is("password"));
        assertThat(userEntries.get("topic-operator").role(), is(ApiCredentials.Role.ADMIN));

        assertThat(userEntries.get("username0").username(), is("username0"));
        assertThat(userEntries.get("username0").password(), is("password0"));
        assertThat(userEntries.get("username0").role(), is(ApiCredentials.Role.USER));

        assertThat(userEntries.get("username1").username(), is("username1"));
        assertThat(userEntries.get("username1").password(), is("password1"));
        assertThat(userEntries.get("username1").role(), is(ApiCredentials.Role.VIEWER));

        CruiseControl cc2 = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, createKafka(null), VERSIONS, NODES, STORAGE, Map.of(), SHARED_ENV_PROVIDER);
        newCcApiUsersSecret = cc2.apiCredentials().generateApiSecret(
                new PasswordGenerator(10, "a", "a"),
                null,
                null,
                null);

        userEntries =  ApiCredentials.parseEntriesFromString(decodeFromBase64(newCcApiUsersSecret.getData().get(AUTH_FILE_KEY)));
        assertThat(userEntries.get("rebalance-operator").username(), is(REBALANCE_OPERATOR_USERNAME));
        assertThat(userEntries.get("rebalance-operator").password(), is(not(emptyString())));
        assertThat(userEntries.get("rebalance-operator").role(), is(ApiCredentials.Role.ADMIN));

        assertThat(userEntries.get("healthcheck").username(), is(HEALTHCHECK_USERNAME));
        assertThat(userEntries.get("healthcheck").password(), is(not(emptyString())));
        assertThat(userEntries.get("healthcheck").role(), is(ApiCredentials.Role.USER));

        assertThat(userEntries.get(TOPIC_OPERATOR_USERNAME_KEY), is(nullValue()));
        assertThat(userEntries.size(), is(2));

        // Ensure exception is thrown when secret referenced in the apiUsers config does not exist
        CruiseControl cc3 = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, createKafka(API_USERS), VERSIONS, NODES, STORAGE, Map.of(), SHARED_ENV_PROVIDER);
        assertThrows(InvalidResourceException.class,  () -> cc3.apiCredentials().generateApiSecret(
                new PasswordGenerator(10, "a", "a"),
                null,
                null,
                null));
    }
}
