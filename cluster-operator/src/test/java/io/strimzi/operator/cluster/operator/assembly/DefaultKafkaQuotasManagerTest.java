/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafkaBuilder;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginStrimzi;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.common.auth.TlsPemIdentity.DUMMY_IDENTITY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class DefaultKafkaQuotasManagerTest {
    private final static Long DEFAULT_PRODUCER_BYTE_RATE = 2000L;
    private final static Long DEFAULT_CONSUMER_BYTE_RATE = 2000L;
    private final static Double DEFAULT_MUTATION_RATE = 0.5;
    private final static Integer DEFAULT_REQUEST_PERCENTAGE = 25;

    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    /**
     * Checks whether {@link DefaultKafkaQuotasManager#currentAndDesiredQuotasDiffer(Map, List)} is able to handle the
     * `null` values in both current and desired quotas and if it correctly returns the result.
     */
    @Test
    void testCurrentAndDesiredQuotasDifferWithNullValuesInBothQuotas() {
        Map<String, Double> currentQuotas = createMapOfCurrentQuotas(null, null, null, null);
        List<ClientQuotaAlteration.Op> desiredQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(DefaultKafkaQuotasManager.emptyQuotasPluginKafka());

        assertThat(DefaultKafkaQuotasManager.currentAndDesiredQuotasDiffer(currentQuotas, desiredQuotas), is(false));
    }

    /**
     * Checks whether {@link DefaultKafkaQuotasManager#currentAndDesiredQuotasDiffer(Map, List)} is able to handle the
     * `null` values in different quotas inside current and desired quotas and if it correctly returns the result.
     */
    @Test
    void testCurrentAndDesiredQuotasDifferWithNullValuesInDifferentQuotas() {
        Map<String, Double> currentQuotas = createMapOfCurrentQuotas(null, 1000L, null, 0.5);
        QuotasPluginKafka desiredQuotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withProducerByteRate(1000L)
            .withRequestPercentage(2)
            .build();

        List<ClientQuotaAlteration.Op> desiredQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(desiredQuotasPluginKafka);

        assertThat(DefaultKafkaQuotasManager.currentAndDesiredQuotasDiffer(currentQuotas, desiredQuotas), is(true));
    }

    /**
     * Checks whether {@link DefaultKafkaQuotasManager#currentAndDesiredQuotasDiffer(Map, List)} is able to correctly compare
     * the current and desired quotas in case that only one (last) quota value is different and if it correctly returns the
     * result.
     */
    @Test
    void testCurrentAndDesiredQuotasDifferWithOneDifferentQuotaValue() {
        Long producerByteRate = 1000L;
        Long consumerByteRate = 1000L;
        Integer requestPercentage = 25;

        Map<String, Double> currentQuotas = createMapOfCurrentQuotas(producerByteRate, consumerByteRate, requestPercentage, 0.5);
        QuotasPluginKafka desiredQuotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withProducerByteRate(producerByteRate)
            .withConsumerByteRate(consumerByteRate)
            .withRequestPercentage(requestPercentage)
            .withControllerMutationRate(0.6)
            .build();

        List<ClientQuotaAlteration.Op> desiredQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(desiredQuotasPluginKafka);

        assertThat(DefaultKafkaQuotasManager.currentAndDesiredQuotasDiffer(currentQuotas, desiredQuotas), is(true));
    }

    /**
     * Checks if the result of {@link DefaultKafkaQuotasManager#shouldAlterDefaultQuotasConfig(Reconciliation, Vertx, Admin, List, boolean)}
     * when there are no default Kafka quotas set and none (or Strimzi Quotas plugin) are desired.
     * The result of this check should be false.
     *
     * @param context   Vertx test context
     */
    @Test
    void testShouldAlterQuotasWithNoneCurrentAndNotAKafkaQuotasPlugin(VertxTestContext context) {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultEmpty(mockAdminClient);

        List<ClientQuotaAlteration.Op> noneQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(DefaultKafkaQuotasManager.emptyQuotasPluginKafka());

        Checkpoint checkpoint = context.checkpoint();

        DefaultKafkaQuotasManager.shouldAlterDefaultQuotasConfig(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClient, noneQuotas, true)
            .onComplete(context.succeeding(result -> {
                assertThat(result, is(false));

                checkpoint.flag();
            }));
    }

    /**
     * Checks if the result of {@link DefaultKafkaQuotasManager#shouldAlterDefaultQuotasConfig(Reconciliation, Vertx, Admin, List, boolean)}
     * when there are no default Kafka quotas set and there are Kafka quotas specified.
     * The result of this check should be true.
     *
     * @param context   Vertx test context
     */
    @Test
    void testShouldAlterQuotasWithNoneCurrentAndFilledKafkaQuotas(VertxTestContext context) {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultEmpty(mockAdminClient);

        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withProducerByteRate(1000L)
            .build();

        List<ClientQuotaAlteration.Op> desiredQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(quotasPluginKafka);

        Checkpoint checkpoint = context.checkpoint();

        DefaultKafkaQuotasManager.shouldAlterDefaultQuotasConfig(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClient, desiredQuotas, false)
            .onComplete(context.succeeding(result -> {
                assertThat(result, is(true));

                checkpoint.flag();
            }));
    }

    /**
     * Checks if the result of {@link DefaultKafkaQuotasManager#shouldAlterDefaultQuotasConfig(Reconciliation, Vertx, Admin, List, boolean)}
     * when there are default Kafka quotas set and none (or Strimzi Quotas plugin) are desired.
     * The result of this check should be true.
     *
     * @param context   Vertx test context
     */
    @Test
    void testShouldAlterQuotasWithFilledCurrentAndNotAKafkaQuotasPlugin(VertxTestContext context) {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultPresent(mockAdminClient);

        List<ClientQuotaAlteration.Op> noneQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(DefaultKafkaQuotasManager.emptyQuotasPluginKafka());

        Checkpoint checkpoint = context.checkpoint();

        DefaultKafkaQuotasManager.shouldAlterDefaultQuotasConfig(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClient, noneQuotas, true)
            .onComplete(context.succeeding(result -> {
                assertThat(result, is(true));

                checkpoint.flag();
            }));
    }

    /**
     * Checks if the result of {@link DefaultKafkaQuotasManager#shouldAlterDefaultQuotasConfig(Reconciliation, Vertx, Admin, List, boolean)}
     * when there are default Kafka quotas set and different Kafka quotas are desired.
     * The result of this check should be true.
     *
     * @param context   Vertx test context
     */
    @Test
    void testShouldAlterQuotasWithFilledCurrentAndDifferentKafkaQuotas(VertxTestContext context) {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultPresent(mockAdminClient);

        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withProducerByteRate(1000L)
            .build();

        List<ClientQuotaAlteration.Op> desiredQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(quotasPluginKafka);

        Checkpoint checkpoint = context.checkpoint();

        DefaultKafkaQuotasManager.shouldAlterDefaultQuotasConfig(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClient, desiredQuotas, false)
            .onComplete(context.succeeding(result -> {
                assertThat(result, is(true));

                checkpoint.flag();
            }));
    }

    /**
     * Checks if the result of {@link DefaultKafkaQuotasManager#shouldAlterDefaultQuotasConfig(Reconciliation, Vertx, Admin, List, boolean)}
     * when there are default Kafka quotas set and same Kafka quotas are desired.
     * The result of this check should be false.
     *
     * @param context   Vertx test context
     */
    @Test
    void testShouldAlterQuotasWithSameCurrentAndDesiredKafkaQuotas(VertxTestContext context) {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultPresent(mockAdminClient);

        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withProducerByteRate(DEFAULT_PRODUCER_BYTE_RATE)
            .withConsumerByteRate(DEFAULT_CONSUMER_BYTE_RATE)
            .withControllerMutationRate(DEFAULT_MUTATION_RATE)
            .withRequestPercentage(DEFAULT_REQUEST_PERCENTAGE)
            .build();

        List<ClientQuotaAlteration.Op> desiredQuotas = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(quotasPluginKafka);

        Checkpoint checkpoint = context.checkpoint();

        DefaultKafkaQuotasManager.shouldAlterDefaultQuotasConfig(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClient, desiredQuotas, false)
            .onComplete(context.succeeding(result -> {
                assertThat(result, is(false));

                checkpoint.flag();
            }));
    }

    /**
     * Tests that the default Kafka quotas are altered in case of different configuration specified by user.
     *
     * @param context   Vertx test context
     */
    @Test
    void testReconfigureDefaultQuotasSetInKafka(VertxTestContext context) {
        long consumerByteRate = 1000;
        long producerByteRate = 1000;
        double mutationRate = 0.1;
        int requestPercentage = 33;

        Checkpoint checkpoint = context.checkpoint();

        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultPresent(mockAdminClient);

        // Mock altering the default Kafka quotas
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<ClientQuotaAlteration>> quotaAlterationCaptor = ArgumentCaptor.forClass(List.class);
        mockAlterQuotas(mockAdminClient, quotaAlterationCaptor);

        // Mock the Admin Client Provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withConsumerByteRate(consumerByteRate)
            .withProducerByteRate(producerByteRate)
            .withControllerMutationRate(mutationRate)
            .withRequestPercentage(requestPercentage)
            .build();

        List<ClientQuotaAlteration.Op> expectedResult = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(quotasPluginKafka);

        // Scenario with configured QuotasPluginKafka and default user quota set in Kafka (all options) -> but with different values in both configurations
        DefaultKafkaQuotasManager.reconcileDefaultUserQuotas(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClientProvider, DUMMY_IDENTITY.pemTrustSet(), DUMMY_IDENTITY.pemAuthIdentity(), quotasPluginKafka)
            .onComplete(context.succeeding(s -> {
                verify(mockAdminClient, times(1)).describeClientQuotas(any());
                verify(mockAdminClient, times(1)).alterClientQuotas(any());

                assertThat(quotaAlterationCaptor.getValue().isEmpty(), is(false));

                List<ClientQuotaAlteration.Op> valuesSet = quotaAlterationCaptor.getValue().get(0).ops().stream().toList();
                assertEquals(expectedResult, valuesSet);

                checkpoint.flag();
            }));
    }

    /**
     * Tests that the default Kafka quotas are set to null (deleted) in case that none quotas plugin is specified by user.
     *
     * @param context   Vertx test context
     */
    @Test
    void testRemoveDefaultQuotasIfNoneQuotasPluginIsDesired(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();

        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultPresent(mockAdminClient);

        // Mock altering the default Kafka quotas
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<ClientQuotaAlteration>> quotaAlterationCaptor = ArgumentCaptor.forClass(List.class);
        mockAlterQuotas(mockAdminClient, quotaAlterationCaptor);

        // Mock the Admin Client Provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        List<ClientQuotaAlteration.Op> expectedResult = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(DefaultKafkaQuotasManager.emptyQuotasPluginKafka());

        // Scenario with configured QuotasPluginKafka and default user quota set in Kafka (all options) -> but with different values in both configurations
        DefaultKafkaQuotasManager.reconcileDefaultUserQuotas(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClientProvider, DUMMY_IDENTITY.pemTrustSet(), DUMMY_IDENTITY.pemAuthIdentity(), null)
            .onComplete(context.succeeding(s -> {
                verify(mockAdminClient, times(1)).describeClientQuotas(any());
                verify(mockAdminClient, times(1)).alterClientQuotas(any());

                assertThat(quotaAlterationCaptor.getValue().isEmpty(), is(false));

                List<ClientQuotaAlteration.Op> valuesSet = quotaAlterationCaptor.getValue().get(0).ops().stream().toList();
                assertEquals(expectedResult, valuesSet);
                checkpoint.flag();
            }));
    }

    /**
     * Tests that the default Kafka quotas are set to null (deleted) in case that Strimzi quotas plugin is specified by user.
     *
     * @param context   Vertx test context
     */
    @Test
    void testRemoveDefaultQuotasIfStrimziQuotasPluginIsDesired(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();

        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeQuotasResultPresent(mockAdminClient);

        // Mock altering the default Kafka quotas
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<ClientQuotaAlteration>> quotaAlterationCaptor = ArgumentCaptor.forClass(List.class);
        mockAlterQuotas(mockAdminClient, quotaAlterationCaptor);

        // Mock the Admin Client Provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        List<ClientQuotaAlteration.Op> expectedResult = DefaultKafkaQuotasManager.prepareQuotaConfigurationRequest(DefaultKafkaQuotasManager.emptyQuotasPluginKafka());

        // Scenario with configured QuotasPluginKafka and default user quota set in Kafka (all options) -> but with different values in both configurations
        DefaultKafkaQuotasManager.reconcileDefaultUserQuotas(Reconciliation.DUMMY_RECONCILIATION, vertx, mockAdminClientProvider, DUMMY_IDENTITY.pemTrustSet(), DUMMY_IDENTITY.pemAuthIdentity(), new QuotasPluginStrimzi())
            .onComplete(context.succeeding(s -> {
                verify(mockAdminClient, times(1)).describeClientQuotas(any());
                verify(mockAdminClient, times(1)).alterClientQuotas(any());

                assertThat(quotaAlterationCaptor.getValue().isEmpty(), is(false));

                List<ClientQuotaAlteration.Op> valuesSet = quotaAlterationCaptor.getValue().get(0).ops().stream().toList();
                assertEquals(expectedResult, valuesSet);
                checkpoint.flag();
            }));
    }

    private Map<String, Double> createMapOfCurrentQuotas(
        Long producerByteRate,
        Long consumerByteRate,
        Integer requestPercentage,
        Double mutationRate
    ) {
        // we cannot pass null to Map.of()
        Map<String, Double> currentQuotas = new HashMap<>();
        currentQuotas.put("producer_byte_rate", producerByteRate == null ? null : Double.valueOf(producerByteRate));
        currentQuotas.put("consumer_byte_rate", consumerByteRate == null ? null : Double.valueOf(consumerByteRate));
        currentQuotas.put("request_percentage", requestPercentage == null ? null : Double.valueOf(requestPercentage));
        currentQuotas.put("controller_mutation_rate", mutationRate);

        return currentQuotas;
    }

    private void mockDescribeQuotasResultPresent(Admin mockAdminClient) {
        Map<String, Double> mockedResult = createMapOfCurrentQuotas(DEFAULT_PRODUCER_BYTE_RATE, DEFAULT_CONSUMER_BYTE_RATE, DEFAULT_REQUEST_PERCENTAGE, DEFAULT_MUTATION_RATE);

        DescribeClientQuotasResult result = mock(DescribeClientQuotasResult.class);
        final ClientQuotaEntity defaultUserEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, null));
        when(result.entities()).thenReturn(KafkaFuture.completedFuture(Map.of(defaultUserEntity, mockedResult)));

        when(mockAdminClient.describeClientQuotas(ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER))))).thenReturn(result);
    }

    private void mockDescribeQuotasResultEmpty(Admin mockAdminClient) {
        DescribeClientQuotasResult result = mock(DescribeClientQuotasResult.class);
        when(result.entities()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(mockAdminClient.describeClientQuotas(ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER))))).thenReturn(result);
    }

    private void mockAlterQuotas(Admin mockAdminClient, ArgumentCaptor<List<ClientQuotaAlteration>> quotaAlterationCaptor) {
        when(mockAdminClient.alterClientQuotas(quotaAlterationCaptor.capture())).thenAnswer(i -> new AlterClientQuotasResult(Map.of()));
    }

    private AdminClientProvider mockAdminClientProvider(Admin adminClient)  {
        AdminClientProvider mockAdminClientProvider = mock(AdminClientProvider.class);
        when(mockAdminClientProvider.createAdminClient(anyString(), any(), any())).thenReturn(adminClient);

        return mockAdminClientProvider;
    }
}
