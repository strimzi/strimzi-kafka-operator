/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.strimzi.operator.common.auth.TlsPemIdentity.DUMMY_IDENTITY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class KRaftMetadataManagerTest {

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
    public void testNoMetadataVersionChange()   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeVersion(mockAdminClient);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, DUMMY_IDENTITY, mockAdminClientProvider, "3.6-IV1", status)
                .toCompletableFuture()
                .join();

        assertThat(status.getKafkaMetadataVersion(), is("3.6-IV1"));

        verify(mockAdminClient, never()).updateFeatures(any(), any());
        verify(mockAdminClient, times(1)).describeFeatures();
    }

    @Test
    public void testSuccessfulMetadataVersionUpgrade()   {
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

        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, DUMMY_IDENTITY, mockAdminClientProvider, "3.6", status)
                .toCompletableFuture()
                .join();

        assertThat(status.getKafkaMetadataVersion(), is("3.6-IV2"));

        verify(mockAdminClient, times(1)).updateFeatures(any(), any());
        verify(mockAdminClient, times(1)).describeFeatures();

        assertThat(updateCaptor.getAllValues().size(), is(1));
        assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).upgradeType(), is(FeatureUpdate.UpgradeType.UPGRADE));
        assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).maxVersionLevel(), is((short) 14));
    }

    @Test
    public void testSuccessfulMetadataVersionDowngrade()   {
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

        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, DUMMY_IDENTITY, mockAdminClientProvider, "3.5", status)
                .toCompletableFuture()
                .join();

        assertThat(status.getKafkaMetadataVersion(), is("3.5-IV2"));

        verify(mockAdminClient, times(1)).updateFeatures(any(), any());
        verify(mockAdminClient, times(1)).describeFeatures();

        assertThat(updateCaptor.getAllValues().size(), is(1));
        assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).upgradeType(), is(FeatureUpdate.UpgradeType.SAFE_DOWNGRADE));
        assertThat(updateCaptor.getValue().get(KRaftMetadataManager.METADATA_VERSION_KEY).maxVersionLevel(), is((short) 11));
    }

    @Test
    public void testUnsuccessfulMetadataVersionChange()   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        mockDescribeVersion(mockAdminClient);

        // Mock updating metadata version
        @SuppressWarnings(value = "unchecked")
        KafkaFuture<Void> kf = mock(KafkaFuture.class);
        when(kf.toCompletionStage()).thenReturn(CompletableFuture.failedFuture(new InvalidUpdateVersionException("Test error ...")));
        UpdateFeaturesResult ufr = mock(UpdateFeaturesResult.class);
        when(ufr.values()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, kf));
        when(mockAdminClient.updateFeatures(any(), any())).thenReturn(ufr);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, DUMMY_IDENTITY, mockAdminClientProvider, "3.6", status)
                .toCompletableFuture()
                .join();

        assertThat(status.getKafkaMetadataVersion(), is("3.6-IV1"));
        assertThat(status.getConditions().size(), is(1));
        assertThat(status.getConditions().get(0).getType(), is("Warning"));
        assertThat(status.getConditions().get(0).getReason(), is("MetadataUpdateFailed"));
        assertThat(status.getConditions().get(0).getMessage(), is("Failed to update metadata version to 3.6"));

        verify(mockAdminClient, times(1)).updateFeatures(any(), any());
        verify(mockAdminClient, times(2)).describeFeatures();
    }

    @Test
    public void testUnexpectedError()   {
        // Mock the Admin client
        Admin mockAdminClient = mock(Admin.class);

        // Mock describing the current metadata version
        @SuppressWarnings(value = "unchecked")
        KafkaFuture<FeatureMetadata> kf = mock(KafkaFuture.class);
        when(kf.toCompletionStage()).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Test error ...")));
        DescribeFeaturesResult dfr = mock(DescribeFeaturesResult.class);
        when(dfr.featureMetadata()).thenReturn(kf);
        when(mockAdminClient.describeFeatures()).thenReturn(dfr);

        // Mock the Admin client provider
        AdminClientProvider mockAdminClientProvider = mockAdminClientProvider(mockAdminClient);

        // Dummy KafkaStatus to check the values from
        KafkaStatus status = new KafkaStatus();

        Exception e = assertThrows(Exception.class, () ->
                KRaftMetadataManager.maybeUpdateMetadataVersion(Reconciliation.DUMMY_RECONCILIATION, DUMMY_IDENTITY, mockAdminClientProvider, "3.5", status)
                        .toCompletableFuture()
                        .join());

        assertThat(e.getCause(), instanceOf(RuntimeException.class));
        assertThat(e.getCause().getMessage(), is("Test error ..."));

        assertThat(status.getKafkaMetadataVersion(), is(nullValue()));

        verify(mockAdminClient, never()).updateFeatures(any(), any());
        verify(mockAdminClient, times(1)).describeFeatures();
    }
}

