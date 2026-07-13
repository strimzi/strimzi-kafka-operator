/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.StatusUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Utility class for managing the KRaft metadata version. It encapsulates the methods for getting the current metadata
 * version and updating it.
 */
public class KRaftMetadataManager {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KRaftMetadataManager.class.getName());
    /* test */ static final String METADATA_VERSION_KEY = MetadataVersion.FEATURE_NAME;

    private KRaftMetadataManager() { }

    /**
     * Utility method for managing the KRaft metadata version.
     *
     * This method will get the current metadata version and compare it with the desired. If needed, it will try to
     * upgrade or downgrade the metadata version to match the desired. It will also update the currently used metadata
     * version in the Kafka CR status.
     *
     * In case this method fails while trying to upgrade the metadata version, it will not fail the whole reconciliation.
     * And instead, it will log the issue at a warning level and add a warning to the .status section of the Kafka
     * custom resource. The currently set metadata version will be kept in the status section as well. The main reason
     * for this is that failures to change the metadata version are expected to happen often. Especially
     * for downgrades, because Kafka support for metadata version downgrades is very limited. And we do not want to stop
     * the whole reconciliation just because of that.
     *
     * Any other failures - such as failure with getting the current metadata version will be reported as errors and
     * would break the reconciliation as usually.
     *
     * This method also never attempts unsafe downgrade. Unsafe downgrade is always expected to be done by the user
     * manually.
     *
     * @param reconciliation            Reconciliation marker
     * @param coTlsPemIdentity          Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     * @param adminClientProvider       Kafka Admin client provider
     * @param desiredMetadataVersion    Desired metadata version
     * @param status                    Kafka status
     *
     * @return  CompletionStage that completes when the metadata update is finished (or was not needed) and the status is updated
     */
    public static CompletionStage<Void> maybeUpdateMetadataVersion(
            Reconciliation reconciliation,
            TlsPemIdentity coTlsPemIdentity,
            AdminClientProvider adminClientProvider,
            String desiredMetadataVersion,
            KafkaStatus status
    ) {
        return maybeUpdateMetadataVersion(reconciliation, coTlsPemIdentity, adminClientProvider, desiredMetadataVersion, status, false);
    }

    /**
     * Utility method for managing the KRaft metadata version with support for deferring any version change.
     *
     * This method behaves like {@link #maybeUpdateMetadataVersion(Reconciliation, TlsPemIdentity, AdminClientProvider, String, KafkaStatus)}.
     * But when {@code deferVersionChange} is true and the current and desired metadata versions differ, the change is
     * not applied. Instead, a warning condition is added to the Kafka custom resource status and the currently used
     * metadata version is kept. This is used while one or more nodes are excluded from automatic rolling updates,
     * because finalizing a metadata version change without the excluded nodes could leave them unable to rejoin the
     * cluster.
     *
     * @param reconciliation            Reconciliation marker
     * @param coTlsPemIdentity          Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     * @param adminClientProvider       Kafka Admin client provider
     * @param desiredMetadataVersion    Desired metadata version
     * @param status                    Kafka status
     * @param deferVersionChange        When true, any metadata version change is deferred instead of applied
     *
     * @return  CompletionStage that completes when the metadata update is finished (or was not needed or was deferred) and the status is updated
     */
    public static CompletionStage<Void> maybeUpdateMetadataVersion(
            Reconciliation reconciliation,
            TlsPemIdentity coTlsPemIdentity,
            AdminClientProvider adminClientProvider,
            String desiredMetadataVersion,
            KafkaStatus status,
            boolean deferVersionChange
    ) {
        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
        LOGGER.debugCr(reconciliation, "Creating AdminClient for Kafka cluster in namespace {}", reconciliation.namespace());
        Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity());

        return maybeUpdateMetadataVersion(reconciliation, kafkaAdmin, desiredMetadataVersion, status, deferVersionChange)
                .whenComplete((result, error) -> {
                    // Close the Admin client and return the original result
                    LOGGER.debugCr(reconciliation, "Closing the Kafka Admin API connection");
                    kafkaAdmin.close();
                });
    }

    private static CompletionStage<Void> maybeUpdateMetadataVersion(
            Reconciliation reconciliation,
            Admin kafkaAdmin,
            String desiredMetadataVersion,
            KafkaStatus status,
            boolean deferVersionChange
    )    {
        short desiredMetadataLevel = MetadataVersion.fromVersionString(desiredMetadataVersion, false).featureLevel();

        return currentVersion(reconciliation, kafkaAdmin)
                .thenCompose(currentMetadataLevel -> {
                    if (currentMetadataLevel.equals(desiredMetadataLevel))  {
                        // No metadata version change as the current and desired versions are equal
                        // We convert the metadata level to the version for the use in the status instead of using the
                        // desired version directly in order to get the full version including the subversion
                        String metadataVersion = MetadataVersion.fromFeatureLevel(currentMetadataLevel).toString();
                        LOGGER.debugCr(reconciliation, "Metadata version is already set to the desired version {}", metadataVersion);
                        status.setKafkaMetadataVersion(MetadataVersion.fromFeatureLevel(currentMetadataLevel).toString());
                        return CompletableFuture.completedFuture(null);
                    } else if (deferVersionChange)  {
                        // A metadata version change is needed, but one or more nodes are excluded from automatic
                        // rolling updates. Changing the metadata version without them could leave them unable to
                        // rejoin the cluster, so the change is deferred until all nodes are managed again.
                        String metadataVersion = MetadataVersion.fromFeatureLevel(currentMetadataLevel).toString();
                        LOGGER.warnCr(reconciliation, "Deferring the metadata version change to {} because one or more nodes are excluded from automatic rolling updates (the current version {} will be kept)", desiredMetadataVersion, metadataVersion);
                        status.addCondition(StatusUtils.buildWarningCondition("MetadataVersionChangeDeferred", "The metadata version change to " + desiredMetadataVersion + " is deferred because one or more nodes are excluded from automatic rolling updates"));
                        status.setKafkaMetadataVersion(metadataVersion);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return updateVersion(reconciliation, kafkaAdmin, currentMetadataLevel, desiredMetadataLevel)
                                .thenCompose(voidResult -> {
                                    // We convert the metadata level to the version for the use in the status instead of using the
                                    // desired version directly in order to get the full version including the subversion
                                    String metadataVersion = MetadataVersion.fromFeatureLevel(desiredMetadataLevel).toString();
                                    LOGGER.infoCr(reconciliation, "Successfully updated metadata version to {}", metadataVersion);
                                    status.setKafkaMetadataVersion(metadataVersion);
                                    return CompletableFuture.<Void>completedFuture(null);
                                })
                                .exceptionallyCompose(error -> {
                                    // We failed to update the metadata version, but we do not want to break the
                                    // reconciliation. We log the error and update the metadata version status.
                                    return currentVersion(reconciliation, kafkaAdmin)
                                            .thenCompose(currentMetadataLevelAfterFailure -> {
                                                String metadataVersion = MetadataVersion.fromFeatureLevel(currentMetadataLevelAfterFailure).toString();
                                                LOGGER.warnCr(reconciliation, "Failed to update metadata version to {} (the current version is {})", desiredMetadataVersion, metadataVersion, error);
                                                status.addCondition(StatusUtils.buildWarningCondition("MetadataUpdateFailed", "Failed to update metadata version to " + desiredMetadataVersion));
                                                status.setKafkaMetadataVersion(metadataVersion);
                                                return CompletableFuture.completedFuture(null);
                                            });
                                });
                    }
                });
    }

    private static CompletionStage<Short> currentVersion(
            Reconciliation reconciliation,
            Admin kafkaAdmin
    )  {
        return kafkaAdmin.describeFeatures().featureMetadata().toCompletionStage()
                .thenCompose(featureMetadata -> {
                    if (featureMetadata.finalizedFeatures().get(METADATA_VERSION_KEY) != null)  {
                        return CompletableFuture.completedFuture(featureMetadata.finalizedFeatures().get(METADATA_VERSION_KEY).maxVersionLevel());
                    } else {
                        return CompletableFuture.failedFuture(new RuntimeException("Failed to describe " + METADATA_VERSION_KEY + " feature"));
                    }
                });
    }

    private static CompletionStage<Void> updateVersion(
            Reconciliation reconciliation,
            Admin kafkaAdmin,
            short currentMetadataLevel,
            short desiredMetadataLevel
    )  {
        UpdateFeaturesOptions options = new UpdateFeaturesOptions().validateOnly(false);
        FeatureUpdate.UpgradeType upgradeType = desiredMetadataLevel > currentMetadataLevel ? FeatureUpdate.UpgradeType.UPGRADE : FeatureUpdate.UpgradeType.SAFE_DOWNGRADE;
        FeatureUpdate featureUpdate = new FeatureUpdate(desiredMetadataLevel, upgradeType);

        LOGGER.infoCr(reconciliation, "Updating metadata version from {} to {}", MetadataVersion.fromFeatureLevel(currentMetadataLevel), MetadataVersion.fromFeatureLevel(desiredMetadataLevel));

        return kafkaAdmin.updateFeatures(Map.of(METADATA_VERSION_KEY, featureUpdate), options).values().get(METADATA_VERSION_KEY).toCompletionStage();
    }
}
