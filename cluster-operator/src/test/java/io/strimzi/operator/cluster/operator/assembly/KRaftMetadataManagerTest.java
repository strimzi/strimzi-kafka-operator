/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static io.strimzi.operator.common.auth.TlsPemIdentity.DUMMY_IDENTITY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KRaftMetadataManagerTest {
    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private AdminClientProvider mockAdminClientProvider(Admin adminClient)  {
        AdminClientProvider mockAdminClientProvider = mock(AdminClientProvider.class);
        when(mockAdminClientProvider.createAdminClient(anyString(), any(), any())).thenReturn(adminClient);

        return mockAdminClientProvider;
    }

    private void mockDescribeVersion(Admin mockAdminClient)   {
        FinalizedVersionRange fvr = mock(FinalizedVersionRange.class);
        when(fvr.maxVersionLevel()).thenReturn((short) 13);

        FeatureMetadata fm = mock(FeatureMetadata.class);
        when(fm.finalizedFeatures()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, fvr));

        DescribeFeaturesResult dfr = mock(DescribeFeaturesResult.class);
        when(dfr.featureMetadata()).thenReturn(KafkaFuture.completedFuture(fm));

        when(mockAdminClient.describeFeatures()).thenReturn(dfr);
    }

    @Test
    public void testNoMetadataVersionChange(VertxTestContext context)   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeVersion(mockAdminClient);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        Checkpoint checkpoint = context.checkpoint();
        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, vertx, DUMMY_IDENTITY, mockAdminClientProvider, "3.6-IV1", status)
                .onComplete(context.succeeding(s -> {
                    assertThat(status.getKafkaMetadataVersion(), is("3.6-IV1"));

                    verify(mockAdminClient, never()).updateFeatures(any(), any());
                    verify(mockAdminClient, times(1)).describeFeatures();

                    checkpoint.flag();
                }));
    }

    @Test
    public void testSuccessfulMetadataVersionUpgrade(VertxTestContext context)   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeVersion(mockAdminClient);

        // Mock updating metadata version
        UpdateFeaturesResult ufr = mock(UpdateFeaturesResult.class);
        when(ufr.values()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, KafkaFuture.completedFuture(null)));
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<Map<String, FeatureUpdate>> updateCaptor = ArgumentCaptor.forClass(Map.class);
        when(mockAdminClient.updateFeatures(updateCaptor.capture(), any())).thenReturn(ufr);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        Checkpoint checkpoint = context.checkpoint();
        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, vertx, DUMMY_IDENTITY, mockAdminClientProvider, "3.6", status)
                .onComplete(context.succeeding(s -> {
                    assertThat(status.getKafkaMetadataVersion(), is("3.6-IV2"));

                    verify(mockAdminClient, times(1)).updateFeatures(any(), any());
                    verify(mockAdminClient, times(1)).describeFeatures();

                    assertThat(updateCaptor.getAllValues().size(), is(1));
                    assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).upgradeType(), is(FeatureUpdate.UpgradeType.UPGRADE));
                    assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).maxVersionLevel(), is((short) 14));

                    checkpoint.flag();
                }));
    }

    @Test
    public void testSuccessfulMetadataVersionDowngrade(VertxTestContext context)   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeVersion(mockAdminClient);

        // Mock updating metadata version
        UpdateFeaturesResult ufr = mock(UpdateFeaturesResult.class);
        when(ufr.values()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, KafkaFuture.completedFuture(null)));
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<Map<String, FeatureUpdate>> updateCaptor = ArgumentCaptor.forClass(Map.class);
        when(mockAdminClient.updateFeatures(updateCaptor.capture(), any())).thenReturn(ufr);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        Checkpoint checkpoint = context.checkpoint();
        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, vertx, DUMMY_IDENTITY, mockAdminClientProvider, "3.5", status)
                .onComplete(context.succeeding(s -> {
                    assertThat(status.getKafkaMetadataVersion(), is("3.5-IV2"));

                    verify(mockAdminClient, times(1)).updateFeatures(any(), any());
                    verify(mockAdminClient, times(1)).describeFeatures();

                    assertThat(updateCaptor.getAllValues().size(), is(1));
                    assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).upgradeType(), is(FeatureUpdate.UpgradeType.SAFE_DOWNGRADE));
                    assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).maxVersionLevel(), is((short) 11));

                    checkpoint.flag();
                }));
    }

    @Test
    public void testUnsuccessfulMetadataVersionChange(VertxTestContext context)   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeVersion(mockAdminClient);

        // Mock updating metadata version
        @SuppressWarnings(value = "unchecked")
        KafkaFuture<Void> kf = mock(KafkaFuture.class);
        when(kf.whenComplete(any())).thenAnswer(i -> {
            KafkaFuture.BiConsumer<Void, Throwable> action = i.getArgument(0);
            action.accept(null, new InvalidUpdateVersionException("Test error ..."));
            return null;
        });
        UpdateFeaturesResult ufr = mock(UpdateFeaturesResult.class);
        when(ufr.values()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, kf));
        when(mockAdminClient.updateFeatures(any(), any())).thenReturn(ufr);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        Checkpoint checkpoint = context.checkpoint();
        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, vertx, DUMMY_IDENTITY, mockAdminClientProvider, "3.6", status)
                .onComplete(context.succeeding(s -> {
                    assertThat(status.getKafkaMetadataVersion(), is("3.6-IV1"));
                    assertThat(status.getConditions().size(), is(1));
                    assertThat(status.getConditions().get(0).getType(), is("Warning"));
                    assertThat(status.getConditions().get(0).getReason(), is("MetadataUpdateFailed"));
                    assertThat(status.getConditions().get(0).getMessage(), is("Failed to update metadata version to 3.6"));

                    verify(mockAdminClient, times(1)).updateFeatures(any(), any());
                    verify(mockAdminClient, times(2)).describeFeatures();

                    checkpoint.flag();
                }));
    }

    @Test
    public void testUnexpectedError(VertxTestContext context)   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        @SuppressWarnings(value = "unchecked")
        KafkaFuture<FeatureMetadata> kf = mock(KafkaFuture.class);
        when(kf.whenComplete(any())).thenAnswer(i -> {
            KafkaFuture.BiConsumer<FeatureMetadata, Throwable> action = i.getArgument(0);
            action.accept(null, new RuntimeException("Test error ..."));
            return null;
        });
        DescribeFeaturesResult dfr = mock(DescribeFeaturesResult.class);
        when(dfr.featureMetadata()).thenReturn(kf);
        when(mockAdminClient.describeFeatures()).thenReturn(dfr);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        Checkpoint checkpoint = context.checkpoint();
        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, vertx, DUMMY_IDENTITY, mockAdminClientProvider, "3.5", status)
                .onComplete(context.failing(s -> {
                    assertThat(s, instanceOf(RuntimeException.class));
                    assertThat(s.getMessage(), is("Test error ..."));

                    assertThat(status.getKafkaMetadataVersion(), is(nullValue()));

                    verify(mockAdminClient, never()).updateFeatures(any(), any());
                    verify(mockAdminClient, times(1)).describeFeatures();

                    checkpoint.flag();
                }));
    }
}

