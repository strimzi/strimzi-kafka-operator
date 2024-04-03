/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaUserOperatorMockTest {
    private final static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private final CertManager mockCertManager = new MockCertManager();

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private SecretOperator secretOps;
    private CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;

    // Kafka mocks and captors
    private SimpleAclOperator aclOps;
    private ArgumentCaptor<String> aclNameCaptor;
    private ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor;
    private ScramCredentialsOperator scramOps;
    private ArgumentCaptor<String> scramNameCaptor;
    private ArgumentCaptor<String> scramCaptor;
    private QuotasOperator quotasOps;
    private ArgumentCaptor<String> quotasNameCaptor;
    private ArgumentCaptor<KafkaUserQuotas> quotasCaptor;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaUserCrd()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        secretOps = new SecretOperator(EXECUTOR, client);
        kafkaUserOps = new CrdOperator<>(EXECUTOR, client, KafkaUser.class, KafkaUserList.class, "KafkaUser");

        mockCaSecrets();
        mockKafka();
    }

    @AfterEach
    public void afterEach() {
        client.namespaces().withName(namespace).delete();
    }

    private void mockCaSecrets() {
        secretOps.resource(namespace, ResourceUtils.createClientsCaCertSecret(namespace)).create();
        secretOps.resource(namespace, ResourceUtils.createClientsCaKeySecret(namespace)).create();
    }

    @SuppressWarnings("unchecked")
    private void mockKafka()    {
        aclOps = mock(SimpleAclOperator.class);
        aclNameCaptor = ArgumentCaptor.forClass(String.class);
        aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(any(), aclNameCaptor.capture(), aclRulesCaptor.capture())).thenAnswer(invocation -> {
            Set<SimpleAclRule> desired = invocation.getArgument(2);

            if (desired == null)    {
                return CompletableFuture.completedStage(ReconcileResult.deleted());
            } else {
                return CompletableFuture.completedStage(ReconcileResult.created(desired));
            }
        });
        when(aclOps.getAllUsers()).thenReturn(CompletableFuture.completedStage(Set.of("acl-user-1", "acl-user-2")));

        scramOps = mock(ScramCredentialsOperator.class);
        scramNameCaptor = ArgumentCaptor.forClass(String.class);
        scramCaptor = ArgumentCaptor.forClass(String.class);
        when(scramOps.reconcile(any(), scramNameCaptor.capture(), scramCaptor.capture())).thenAnswer(invocation -> {
            String desired = invocation.getArgument(2);

            if (desired == null)    {
                return CompletableFuture.completedStage(ReconcileResult.deleted());
            } else {
                return CompletableFuture.completedStage(ReconcileResult.patched(desired));
            }
        });
        when(scramOps.getAllUsers()).thenReturn(CompletableFuture.completedStage(List.of("scram-user-1", "scram-user-2")));

        quotasOps = mock(QuotasOperator.class);
        quotasNameCaptor = ArgumentCaptor.forClass(String.class);
        quotasCaptor = ArgumentCaptor.forClass(KafkaUserQuotas.class);
        when(quotasOps.reconcile(any(), quotasNameCaptor.capture(), quotasCaptor.capture())).thenAnswer(invocation -> {
            KafkaUserQuotas desired = invocation.getArgument(2);

            if (desired == null)    {
                return CompletableFuture.completedStage(ReconcileResult.deleted());
            } else {
                return CompletableFuture.completedStage(ReconcileResult.patched(desired));
            }
        });
        when(quotasOps.getAllUsers()).thenReturn(CompletableFuture.completedStage(Set.of("quotas-user-1", "quotas-user-2")));
    }

    @Test
    public void testCreateTlsUser() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("ca.crt")), is("clients-ca-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.crt")), is(MockCertManager.userCert()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.key")), is(MockCertManager.userKey()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testCreateExternalTlsUser() throws ExecutionException, InterruptedException {
        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls(namespace))
                .editSpec()
                    .withNewKafkaUserTlsExternalClientAuthentication()
                    .endKafkaUserTlsExternalClientAuthentication()
                .endSpec()
                .build();
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(nullValue()));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testCreateTlsUserWithACLsDisabled() throws ExecutionException, InterruptedException {
        KafkaUser user = new KafkaUserBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafkaUserTlsClientAuthentication()
                    .endKafkaUserTlsClientAuthentication()
                    .withNewQuotas()
                        .withConsumerByteRate(1_024 * 1_024)
                        .withProducerByteRate(1_024 * 1_024)
                    .endQuotas()
                .endSpec()
                .build();
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace, Map.of(), false, "32", null), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("ca.crt")), is("clients-ca-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.crt")), is(MockCertManager.userCert()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.key")), is(MockCertManager.userKey()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(0));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(0));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testCreateScramShaUser() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserScramSha(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)), is(notNullValue()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)).length(), is(32));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG)), is(notNullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(1), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(1), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(0), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(notNullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(1).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0), is(nullValue()));
    }

    @Test
    public void testCreateScramShaUserWithConfigurableLength() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserScramSha(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace, "30"), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)), is(notNullValue()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)).length(), is(30));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG)), is(notNullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(1), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(1), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(0), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(notNullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(1).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0), is(nullValue()));
    }

    @Test
    public void testCreateScramShaUserWithProvidedPassword() throws ExecutionException, InterruptedException {
        // Mock the password secret
        final String desiredPassword = "12345678";
        Secret desiredPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .withNamespace(namespace)
                .endMetadata()
                .addToData("my-password", Base64.getEncoder().encodeToString(desiredPassword.getBytes(StandardCharsets.UTF_8)))
                .build();
        secretOps.resource(namespace, desiredPasswordSecret).create();

        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha(namespace))
            .editSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                    .withNewPassword()
                        .withNewValueFrom()
                            .withNewSecretKeyRef("my-password", "my-secret", false)
                        .endValueFrom()
                    .endPassword()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec()
            .build();
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)), is(desiredPassword));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG)), is("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"12345678\";"));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(1), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(1), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(0), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(notNullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(1).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0), is(nullValue()));
    }

    @Test
    public void testCreateScramShaUserWithMissingPassword() {
        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha(namespace))
            .editSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                    .withNewPassword()
                        .withNewValueFrom()
                            .withNewSecretKeyRef("my-password", "my-secret", false)
                        .endValueFrom()
                    .endPassword()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec()
            .build();
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);

        // Assert reconciliation failed with exception
        ExecutionException thrown = assertThrows(ExecutionException.class, futureResult.toCompletableFuture()::get);
        Throwable rootCause = Util.unwrap(thrown.getCause());
        assertInstanceOf(InvalidResourceException.class, rootCause);
    }

    @Test
    public void testCreateTlsUserWithSecretPrefix() throws ExecutionException, InterruptedException {
        String secretPrefix = "my-test-";
        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace, Map.of(), true, "32", secretPrefix), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(secretPrefix + ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        // The Secret without prefix does not exist
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // The prefixed secret exists
        Secret prefixedUserSecret = secretOps.get(namespace, secretPrefix + ResourceUtils.NAME);
        assertThat(prefixedUserSecret.getMetadata().getName(), is(secretPrefix + user.getMetadata().getName()));
        assertThat(prefixedUserSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(prefixedUserSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(prefixedUserSecret.getData().get("ca.crt")), is("clients-ca-crt"));
        assertThat(Util.decodeFromBase64(prefixedUserSecret.getData().get("user.crt")), is(MockCertManager.userCert()));
        assertThat(Util.decodeFromBase64(prefixedUserSecret.getData().get("user.key")), is(MockCertManager.userKey()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testUpdateTlsUserWithoutChange() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("ca.crt")), is("clients-ca-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.crt")), is("expected-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.key")), is("expected-key"));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    // Tests what happens when for an existing user, ACLs and Quotas are disabled
    @Test
    public void testUpdateTlsUserNoAuthorizationNoQuotas() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user.getSpec().setAuthorization(null);
        user.getSpec().setQuotas(null);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("ca.crt")), is("clients-ca-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.crt")), is("expected-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.key")), is("expected-key"));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), is(nullValue()));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testUpdateTlsUserToTlsExternal() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls(namespace))
                .editSpec()
                    .withNewKafkaUserTlsExternalClientAuthentication()
                    .endKafkaUserTlsExternalClientAuthentication()
                .endSpec()
                .build();
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testUpdateTlsUserWithCAChange() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        // Mock changed CA
        secretOps.resource(namespace, ResourceUtils.CA_CERT_NAME).edit(caSecret -> new SecretBuilder(caSecret)
                .withData(Map.of("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes())))
                .build());

        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("ca.crt")), is("different-clients-ca-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.crt")), is(MockCertManager.userCert()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.key")), is(MockCertManager.userKey()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testUpdateTlsUserWithoutSecret() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("ca.crt")), is("clients-ca-crt"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.crt")), startsWith("-----BEGIN CERTIFICATE-----"));
        assertThat(Util.decodeFromBase64(userSecret.getData().get("user.key")), startsWith("-----BEGIN PRIVATE KEY-----"));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(0), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testUpdateScramShaUserWithoutChange() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretScramSha(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUser user = ResourceUtils.createKafkaUserScramSha(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)), is(notNullValue()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)).length(), is(11));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG)), is(notNullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(1), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(1), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(0), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(notNullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(1).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0), is(nullValue()));
    }

    @Test
    public void testUpdateScramShaUserWithoutSecret() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserScramSha(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret.getMetadata().getName(), is(user.getMetadata().getName()));
        assertThat(userSecret.getMetadata().getNamespace(), is(user.getMetadata().getNamespace()));
        assertThat(userSecret.getMetadata().getLabels(),
                is(Labels.fromMap(user.getMetadata().getLabels())
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)), is(notNullValue()));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD)).length(), is(32));
        assertThat(Util.decodeFromBase64(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG)), is(notNullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(1), hasSize(ResourceUtils.createExpectedSimpleAclRules(user).size()));
        assertThat(capturedACLs.get(1), is(ResourceUtils.createExpectedSimpleAclRules(user)));
        assertThat(capturedACLs.get(0), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(notNullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(1).getConsumerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(1).getProducerByteRate(), is(1_024 * 1_024));
        assertThat(capturedQuotas.get(0), is(nullValue()));
    }

    @Test
    public void testDeleteTlsUser() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), is(nullValue()));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testDeleteTlsUserWithDisabledACLs() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace, Map.of(), false, "32", null), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(0));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(0));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testDeleteTlsUserWithoutSecret() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls(namespace);
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), is(nullValue()));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testDeleteTlsUserWithSecretPrefix() throws ExecutionException, InterruptedException {
        String secretPrefix = "my-test-";
        Secret existingUserSecret = new SecretBuilder(ResourceUtils.createUserSecretTls(namespace))
                .editMetadata()
                    .withName(secretPrefix + ResourceUtils.NAME)
                .endMetadata()
                .build();
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace, Map.of(), true, "32", secretPrefix), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        // The Secret without prefix does not exist
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // The prefixed secret exists
        Secret prefixedUserSecret = secretOps.get(namespace, secretPrefix + ResourceUtils.NAME);
        assertThat(prefixedUserSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), is(nullValue()));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testDeleteExternalTlsUser() throws ExecutionException, InterruptedException {
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), null, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), is(nullValue()));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testDeleteScramShaUser() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretScramSha(namespace);
        secretOps.resource(namespace, existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = secretOps.get(namespace, ResourceUtils.NAME);
        assertThat(userSecret, is(nullValue()));

        // Assert the mocked Kafka ACLs
        List<String> capturedAclNames = aclNameCaptor.getAllValues();
        assertThat(capturedAclNames, hasSize(2));
        assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<Set<SimpleAclRule>> capturedACLs = aclRulesCaptor.getAllValues();
        assertThat(capturedACLs, hasSize(2));
        assertThat(capturedACLs.get(0), is(nullValue()));
        assertThat(capturedACLs.get(1), is(nullValue()));

        // Assert the mocked Kafka SCRAM-SHA users
        List<String> capturedScramNames = scramNameCaptor.getAllValues();
        assertThat(capturedScramNames, hasSize(1));
        assertThat(capturedScramNames.get(0), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(1));
        assertThat(capturedScrams.get(0), is(nullValue()));

        // Assert the mocked Kafka Quotas
        List<String> capturedQuotaNames = quotasNameCaptor.getAllValues();
        assertThat(capturedQuotaNames, hasSize(2));
        assertThat(capturedQuotaNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME)));
        assertThat(capturedQuotaNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME)));

        List<KafkaUserQuotas> capturedQuotas = quotasCaptor.getAllValues();
        assertThat(capturedQuotas, hasSize(2));
        assertThat(capturedQuotas.get(0), is(nullValue()));
        assertThat(capturedQuotas.get(1), is(nullValue()));
    }

    @Test
    public void testStatusNotReadyWhenACLReconciliationFails() {
        // Mock the failure
        when(aclOps.reconcile(any(), aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(CompletableFuture.failedStage(new KafkaException("Something failed!")));

        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);

        ExecutionException e = assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());
        assertThat(e.getCause().getMessage(), containsString("Something failed!"));
    }

    @Test
    public void testReconcileAll() throws ExecutionException, InterruptedException {
        KafkaUser user1 = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls(namespace))
                .editMetadata()
                    .withName("cr-user-1")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(namespace).resource(user1).create();

        KafkaUser user2 = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha(namespace))
                .editMetadata()
                    .withName("cr-user-2")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(namespace).resource(user2).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<Set<NamespaceAndName>> futureResult = op.getAllUsers(namespace);
        Set<NamespaceAndName> users = futureResult.toCompletableFuture().get();

        assertThat(users.size(), is(8));

        users.forEach(user -> assertThat(user.getNamespace(), is(namespace)));

        Set<String> usernames = users.stream().map(NamespaceAndName::getName).collect(Collectors.toSet());
        assertThat(usernames, is(Set.of("quotas-user-2", "quotas-user-1", "acl-user-1", "cr-user-2", "cr-user-1", "scram-user-1", "acl-user-2", "scram-user-2")));
    }

    @Test
    public void testReconcileAllWithoutACLs() throws ExecutionException, InterruptedException {
        KafkaUser user1 = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls(namespace))
                .editMetadata()
                    .withName("cr-user-1")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(namespace).resource(user1).create();

        KafkaUser user2 = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha(namespace))
                .editMetadata()
                    .withName("cr-user-2")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(namespace).resource(user2).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace, Map.of(), false, "32", null), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
        CompletionStage<Set<NamespaceAndName>> futureResult = op.getAllUsers(namespace);
        Set<NamespaceAndName> users = futureResult.toCompletableFuture().get();

        assertThat(users.size(), is(6));

        users.forEach(user -> assertThat(user.getNamespace(), is(namespace)));

        Set<String> usernames = users.stream().map(NamespaceAndName::getName).collect(Collectors.toSet());
        assertThat(usernames, is(Set.of("quotas-user-2", "quotas-user-1", "cr-user-2", "cr-user-1", "scram-user-1", "scram-user-2")));
    }

    @Test
    public void testReconciliationFailsWithDisabledAclOperator() {
        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, new DisabledSimpleAclOperator());
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);

        ExecutionException e = assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());
        assertThat(e.getCause().getMessage(), containsString("DisabledSimpleAclOperator cannot be used to reconcile users"));
    }

    @Test
    public void testReconciliationFailsWithMissingCaCertSecrets()   {
        testReconciliationFailsWithMissingCaSecrets(ResourceUtils.CA_CERT_NAME);
    }

    @Test
    public void testReconciliationFailsWithMissingCaKeySecrets()   {
        testReconciliationFailsWithMissingCaSecrets(ResourceUtils.CA_KEY_NAME);
    }

    // Utility method for resting with missing CA secret
    private void testReconciliationFailsWithMissingCaSecrets(String missingSecretName) {
        KafkaUser user = ResourceUtils.createKafkaUserTls(namespace);
        user = Crds.kafkaUserOperation(client).resource(user).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(namespace), mockCertManager, secretOps, kafkaUserOps, scramOps, quotasOps, new DisabledSimpleAclOperator());
        secretOps.resource(namespace, missingSecretName).delete();
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, namespace, ResourceUtils.NAME), user, null);

        ExecutionException thrown = assertThrows(ExecutionException.class, futureResult.toCompletableFuture()::get);
        Throwable rootCause = Util.unwrap(thrown.getCause());
        assertInstanceOf(InvalidConfigurationException.class, rootCause);
        assertThat(rootCause.getMessage(), containsString(missingSecretName));
    }
}
