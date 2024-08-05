/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.operator.common.Util.encodeToBase64;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ApiCredentialTest {

    private static final String SECRET_NAME = "secretName";
    private static final String SECRET_KEY = "secretKey";

    private static Secret createSecret(Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(SECRET_NAME)
                .endMetadata()
                .addToData(data)
                .build();
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
        Map<String, ApiCredentials.Entry> entries =  ApiCredentials.parseEntriesFromString(config);
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
        Secret secret = createSecret(Map.of("topic-operator.username",  encodeToBase64("topic-operator"),
                "topic-operator.password",  encodeToBase64("password")));
        Map<String, ApiCredentials.Entry> entries =  ApiCredentials.generateToManagedApiCredentials(secret);
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
        Map<String, ApiCredentials.Entry> entries =  ApiCredentials.generateUserManagedApiCredentials(secret, SECRET_KEY);

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
        Map<String, ApiCredentials.Entry> entries =  ApiCredentials.generateCoManagedApiCredentials(mockPasswordGenerator, createSecret(map1));
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
        entries =  ApiCredentials.generateCoManagedApiCredentials(mockPasswordGenerator, null);
        assertThat(entries.get("rebalance-operator").username(), is(CruiseControlApiProperties.REBALANCE_OPERATOR_USERNAME));
        assertThat(entries.get("rebalance-operator").password(), is(not(emptyString())));
        assertThat(entries.get("rebalance-operator").role(), is(ApiCredentials.Role.ADMIN));

        assertThat(entries.get("healthcheck").username(), is(CruiseControlApiProperties.HEALTHCHECK_USERNAME));
        assertThat(entries.get("healthcheck").password(), is(not(emptyString())));
        assertThat(entries.get("healthcheck").role(), is(ApiCredentials.Role.USER));
    }
}
