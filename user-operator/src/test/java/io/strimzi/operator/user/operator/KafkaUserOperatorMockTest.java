/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.test.mockkube2.MockKube2;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
public class KafkaUserOperatorMockTest {
    public static final String NAMESPACE = "namespace";

    private final static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private final CertManager mockCertManager = new MockCertManager();

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;

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

    @BeforeEach
    public void beforeEach() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaUserCrd()
                .build();
        mockKube.start();

        mockCaSecrets();
        mockKafka();
    }

    private void mockCaSecrets()    {
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(ResourceUtils.createClientsCaCertSecret()).create();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(ResourceUtils.createClientsCaKeySecret()).create();
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

    @AfterEach
    public void afterEach() {
        mockKube.stop();
    }

    @Test
    public void testCreateTlsUser() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is(MockCertManager.userCert()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is(MockCertManager.userKey()));

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
        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls())
                .editSpec()
                    .withNewKafkaUserTlsExternalClientAuthentication()
                    .endKafkaUserTlsExternalClientAuthentication()
                .endSpec()
                .build();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(nullValue()));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
                    .withNamespace(ResourceUtils.NAMESPACE)
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
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), false, false, "32", null), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is(MockCertManager.userCert()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is(MockCertManager.userKey()));

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
    public void testCreateTlsUserWithKRaft() throws ExecutionException, InterruptedException {
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), true, true, "32", null), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is(MockCertManager.userCert()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is(MockCertManager.userKey()));

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
        assertThat(capturedScramNames, hasSize(0));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(0));

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
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(notNullValue()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))).length(), is(32));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))), is(notNullValue()));

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
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig("30"), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(notNullValue()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))).length(), is(30));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))), is(notNullValue()));

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
                .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .addToData("my-password", Base64.getEncoder().encodeToString(desiredPassword.getBytes(StandardCharsets.UTF_8)))
                .build();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(desiredPasswordSecret).create();

        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha())
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
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(desiredPassword));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))), is("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"12345678\";"));

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
    public void testCreateTlsUserWithSecretPrefix() throws ExecutionException, InterruptedException {
        String secretPrefix = "my-test-";
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), true, false, "32", secretPrefix), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);
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
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
        assertThat(userSecret, is(nullValue()));

        // The prefixed secret exists
        Secret prefixedUserSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(secretPrefix + ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(prefixedUserSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(prefixedUserSecret.getData().get("user.crt"))), is(MockCertManager.userCert()));
        assertThat(new String(Base64.getDecoder().decode(prefixedUserSecret.getData().get("user.key"))), is(MockCertManager.userKey()));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is("expected-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is("expected-key"));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.getSpec().setAuthorization(null);
        user.getSpec().setQuotas(null);

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is("expected-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is("expected-key"));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUser user = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls())
                .editSpec()
                    .withNewKafkaUserTlsExternalClientAuthentication()
                    .endKafkaUserTlsExternalClientAuthentication()
                .endSpec()
                .build();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        // Mock changed CA
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.CA_CERT_NAME).edit(caSecret -> new SecretBuilder(caSecret)
                .withData(Map.of("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes())))
                .build());

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("different-clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is(MockCertManager.userCert()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is(MockCertManager.userKey()));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is("CN=" + ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("ca.crt"))), is("clients-ca-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.crt"))), is("expected-crt"));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get("user.key"))), is("expected-key"));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretScramSha();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(notNullValue()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))).length(), is(11));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))), is(notNullValue()));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretScramSha();
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(notNullValue()));
        assertThat(status.getUsername(), is(ResourceUtils.NAME));
        assertThat(status.getSecret(), is(ResourceUtils.NAME));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(notNullValue()));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_PASSWORD))).length(), is(11));
        assertThat(new String(Base64.getDecoder().decode(userSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))), is(notNullValue()));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), false, false, "32", null), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
    public void testDeleteTlsUserWithKRaft() throws ExecutionException, InterruptedException {
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), true, true, "32", null), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        assertThat(capturedScramNames, hasSize(0));

        List<String> capturedScrams = scramCaptor.getAllValues();
        assertThat(capturedScrams, hasSize(0));

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
        Secret existingUserSecret = ResourceUtils.createUserSecretTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        Secret existingUserSecret = new SecretBuilder(ResourceUtils.createUserSecretTls())
                .editMetadata()
                    .withName(secretPrefix + ResourceUtils.NAME)
                .endMetadata()
                .build();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), true, false, "32", secretPrefix), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        // The Secret without prefix does not exist
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
        assertThat(userSecret, is(nullValue()));

        // The prefixed secret exists
        Secret prefixedUserSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(secretPrefix + ResourceUtils.NAME).get();
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
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, null);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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
        Secret existingUserSecret = ResourceUtils.createUserSecretScramSha();
        client.secrets().inNamespace(ResourceUtils.NAMESPACE).resource(existingUserSecret).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), null, existingUserSecret);
        KafkaUserStatus status = futureResult.toCompletableFuture().get();

        // Assert KafkaUserStatus
        assertThat(status, is(nullValue()));

        // Assert the user secret
        Secret userSecret = client.secrets().inNamespace(ResourceUtils.NAMESPACE).withName(ResourceUtils.NAME).get();
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

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);

        ExecutionException e = assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());
        assertThat(e.getCause().getMessage(), containsString("Something failed!"));
    }

    @Test
    public void testReconcileAll() throws ExecutionException, InterruptedException {
        KafkaUser user1 = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls())
                .editMetadata()
                    .withName("cr-user-1")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(ResourceUtils.NAMESPACE).resource(user1).create();

        KafkaUser user2 = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha())
                .editMetadata()
                    .withName("cr-user-2")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(ResourceUtils.NAMESPACE).resource(user2).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<Set<NamespaceAndName>> futureResult = op.getAllUsers(ResourceUtils.NAMESPACE);
        Set<NamespaceAndName> users = futureResult.toCompletableFuture().get();

        assertThat(users.size(), is(8));

        users.forEach(user -> assertThat(user.getNamespace(), is(ResourceUtils.NAMESPACE)));

        Set<String> usernames = users.stream().map(NamespaceAndName::getName).collect(Collectors.toSet());
        assertThat(usernames, is(Set.of("quotas-user-2", "quotas-user-1", "acl-user-1", "cr-user-2", "cr-user-1", "scram-user-1", "acl-user-2", "scram-user-2")));
    }

    @Test
    public void testReconcileAllWithoutACLs() throws ExecutionException, InterruptedException {
        KafkaUser user1 = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls())
                .editMetadata()
                    .withName("cr-user-1")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(ResourceUtils.NAMESPACE).resource(user1).create();

        KafkaUser user2 = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha())
                .editMetadata()
                    .withName("cr-user-2")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(ResourceUtils.NAMESPACE).resource(user2).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), false, false, "32", null), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<Set<NamespaceAndName>> futureResult = op.getAllUsers(ResourceUtils.NAMESPACE);
        Set<NamespaceAndName> users = futureResult.toCompletableFuture().get();

        assertThat(users.size(), is(6));

        users.forEach(user -> assertThat(user.getNamespace(), is(ResourceUtils.NAMESPACE)));

        Set<String> usernames = users.stream().map(NamespaceAndName::getName).collect(Collectors.toSet());
        assertThat(usernames, is(Set.of("quotas-user-2", "quotas-user-1", "cr-user-2", "cr-user-1", "scram-user-1", "scram-user-2")));
    }

    @Test
    public void testReconcileAllWithKRaft() throws ExecutionException, InterruptedException {
        KafkaUser user1 = new KafkaUserBuilder(ResourceUtils.createKafkaUserTls())
                .editMetadata()
                    .withName("cr-user-1")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(ResourceUtils.NAMESPACE).resource(user1).create();

        KafkaUser user2 = new KafkaUserBuilder(ResourceUtils.createKafkaUserScramSha())
                .editMetadata()
                    .withName("cr-user-2")
                .endMetadata()
                .build();
        Crds.kafkaUserOperation(client).inNamespace(ResourceUtils.NAMESPACE).resource(user2).create();

        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(Map.of(), true, true, "32", null), client, mockCertManager, scramOps, quotasOps, aclOps, EXECUTOR);
        CompletionStage<Set<NamespaceAndName>> futureResult = op.getAllUsers(ResourceUtils.NAMESPACE);
        Set<NamespaceAndName> users = futureResult.toCompletableFuture().get();

        assertThat(users.size(), is(6));

        users.forEach(user -> assertThat(user.getNamespace(), is(ResourceUtils.NAMESPACE)));

        Set<String> usernames = users.stream().map(NamespaceAndName::getName).collect(Collectors.toSet());
        assertThat(usernames, is(Set.of("quotas-user-2", "quotas-user-1", "cr-user-2", "cr-user-1", "acl-user-1", "acl-user-2")));
    }

    @Test
    public void testReconciliationFailsWithDisabledAclOperator() {
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, scramOps, quotasOps, new DisabledSimpleAclOperator(), EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);

        ExecutionException e = assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());
        assertThat(e.getCause().getMessage(), containsString("DisabledSimpleAclOperator cannot be used to reconcile users"));
    }

    @Test
    public void testReconciliationFailsWithDisabledScramShaOperator() {
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        KafkaUserOperator op = new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(), client, mockCertManager, new DisabledScramCredentialsOperator(), quotasOps, aclOps, EXECUTOR);
        CompletionStage<KafkaUserStatus> futureResult = op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, null);

        ExecutionException e = assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());
        assertThat(e.getCause().getMessage(), containsString("DisabledScramCredentialsOperator cannot be used to reconcile users"));
    }
}
