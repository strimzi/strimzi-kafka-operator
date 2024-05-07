/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Map;

/**
 * Utility class for managing the KRaft metadata version. It encapsulates the methods for getting the current metadata
 * version and updating it.
 */
public class KRaftMetadataManager {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KRaftMetadataManager.class.getName());
    /* test */ static final String METADATA_VERSION_KEY = MetadataVersion.FEATURE_NAME;

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
     * @param vertx                     Vert.x instance
     * @param coTlsPemIdentity          Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     * @param adminClientProvider       Kafka Admin client provider
     * @param desiredMetadataVersion    Desired metadata version
     * @param status                    Kafka status
     *
     * @return  Future that completes when the metadata update is finished (or was not needed) and the status is updated
     */
    public static Future<Void> maybeUpdateMetadataVersion(
            Reconciliation reconciliation,
            Vertx vertx,
            TlsPemIdentity coTlsPemIdentity,
            AdminClientProvider adminClientProvider,
            String desiredMetadataVersion,
            KafkaStatus status
    ) {
        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
        LOGGER.debugCr(reconciliation, "Creating AdminClient for Kafka cluster in namespace {}", reconciliation.namespace());
        Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity());

        Promise<Void> updatePromise = Promise.promise();
        maybeUpdateMetadataVersion(reconciliation, vertx, kafkaAdmin, desiredMetadataVersion, status)
                .onComplete(res -> {
                    // Close the Admin client and return the original result
                    LOGGER.debugCr(reconciliation, "Closing the Kafka Admin API connection");
                    kafkaAdmin.close();
                    updatePromise.handle(res);
                });

        return updatePromise.future();
    }

    private static Future<Void> maybeUpdateMetadataVersion(
            Reconciliation reconciliation,
            Vertx vertx,
            Admin kafkaAdmin,
            String desiredMetadataVersion,
            KafkaStatus status
    )    {
        short desiredMetadataLevel = MetadataVersion.fromVersionString(desiredMetadataVersion).featureLevel();

        return currentVersion(reconciliation, vertx, kafkaAdmin)
                .compose(currentMetadataLevel -> {
                    if (currentMetadataLevel.equals(desiredMetadataLevel))  {
                        // No metadata version change as the current and desired versions are equal
                        // We convert the metadata level to the version for the use in the status instead of using the
                        // desired version directly in order to get the full version including the subversion
                        String metadataVersion = MetadataVersion.fromFeatureLevel(currentMetadataLevel).toString();
                        LOGGER.debugCr(reconciliation, "Metadata version is already set to the desired version {}", metadataVersion);
                        status.setKafkaMetadataVersion(MetadataVersion.fromFeatureLevel(currentMetadataLevel).toString());
                        return Future.succeededFuture();
                    } else {
                        Promise<Void> promise = Promise.promise();

                        updateVersion(reconciliation, vertx, kafkaAdmin, currentMetadataLevel, desiredMetadataLevel)
                                .onComplete(res -> {
                                    if (res.succeeded())    {
                                        // We convert the metadata level to the version for the use in the status instead of using the
                                        // desired version directly in order to get the full version including the subversion
                                        String metadataVersion = MetadataVersion.fromFeatureLevel(desiredMetadataLevel).toString();
                                        LOGGER.infoCr(reconciliation, "Successfully updated metadata version to {}", metadataVersion);
                                        status.setKafkaMetadataVersion(metadataVersion);
                                        promise.complete();
                                    } else {
                                        // We failed to update the metadata version, but we do not want to break the
                                        // reconciliation. We log the error and update the metadata version status.
                                        currentVersion(reconciliation, vertx, kafkaAdmin)
                                                .compose(currentMetadataLevelAfterFailure -> {
                                                    String metadataVersion = MetadataVersion.fromFeatureLevel(currentMetadataLevelAfterFailure).toString();
                                                    LOGGER.warnCr(reconciliation, "Failed to update metadata version to {} (the current version is {})", desiredMetadataVersion, metadataVersion, res.cause());
                                                    status.addCondition(StatusUtils.buildWarningCondition("MetadataUpdateFailed", "Failed to update metadata version to " + desiredMetadataVersion));
                                                    status.setKafkaMetadataVersion(metadataVersion);

                                                    promise.complete();
                                                    return Future.succeededFuture();
                                                });
                                    }
                                });

                        return promise.future();
                    }
                });
    }

    private static Future<Short> currentVersion(
            Reconciliation reconciliation,
            Vertx vertx,
            Admin kafkaAdmin
    )  {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, kafkaAdmin.describeFeatures().featureMetadata())
                .compose(featureMetadata -> {
                    if (featureMetadata.finalizedFeatures().get(METADATA_VERSION_KEY) != null)  {
                        return Future.succeededFuture(featureMetadata.finalizedFeatures().get(METADATA_VERSION_KEY).maxVersionLevel());
                    } else {
                        return Future.failedFuture("Failed to describe " + METADATA_VERSION_KEY + " feature");
                    }
                });
    }

    private static Future<Void> updateVersion(
            Reconciliation reconciliation,
            Vertx vertx,
            Admin kafkaAdmin,
            short currentMetadataLevel,
            short desiredMetadataLevel
    )  {
        UpdateFeaturesOptions options = new UpdateFeaturesOptions().validateOnly(false);
        FeatureUpdate.UpgradeType upgradeType = desiredMetadataLevel > currentMetadataLevel ? FeatureUpdate.UpgradeType.UPGRADE : FeatureUpdate.UpgradeType.SAFE_DOWNGRADE;
        FeatureUpdate featureUpdate = new FeatureUpdate(desiredMetadataLevel, upgradeType);

        LOGGER.infoCr(reconciliation, "Updating metadata version from {} to {}", MetadataVersion.fromFeatureLevel(currentMetadataLevel), MetadataVersion.fromFeatureLevel(desiredMetadataLevel));

        return VertxUtil
                .kafkaFutureToVertxFuture(reconciliation, vertx, kafkaAdmin.updateFeatures(Map.of(METADATA_VERSION_KEY, featureUpdate), options).values().get(METADATA_VERSION_KEY))
                .map((Void) null);
    }
}
