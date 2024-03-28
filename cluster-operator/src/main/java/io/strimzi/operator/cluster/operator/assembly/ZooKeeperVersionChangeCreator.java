/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;

import java.util.List;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedIVVersions;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;

/**
 * Creates the KafkaVersionChange object from the different versions in the Kafka CR, in the StatefulSet / PodSet and
 * on the pods.
 */
public class ZooKeeperVersionChangeCreator implements VersionChangeCreator {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZooKeeperVersionChangeCreator.class.getName());

    private final Reconciliation reconciliation;
    private final KafkaVersion.Lookup versions;

    private final StatefulSetOperator stsOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final PodOperator podOperator;

    // Information about Kafka version from the Kafka custom resource
    private final KafkaVersion versionFromCr;
    private final String metadataVersionFromCr;
    private final String interBrokerProtocolVersionFromCr;
    private final String logMessageFormatVersionFromCr;

    // Kafka version extracted from the controller resource (StatefulSet or StrimziPodSet)
    private String versionFromControllerResource = null;
    // Indicates whether this is initial deployment of whether the StatefulSet or PodSet already exist
    private boolean freshDeployment = true;
    private String highestInterBrokerProtocolVersionFromPods = null;
    private String highestLogMessageFormatVersionFromPods = null;

    // The target versions which should be set in the result
    private KafkaVersion versionFrom;
    private KafkaVersion versionTo;
    private String logMessageFormatVersion;
    private String interBrokerProtocolVersion;

    /**
     * Constructs the ZooKeeperVersionChangeCreator which constructs the KafkaVersionChange instance which is describing the
     * upgrade state.
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr           The Kafka custom resource
     * @param config            Cluster Operator Configuration
     * @param supplier          Supplier with Kubernetes Resource Operators
     */
    public ZooKeeperVersionChangeCreator(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;

        // Collect information from the Kafka CR
        this.versionFromCr = config.versions().supportedVersion(kafkaCr.getSpec().getKafka().getVersion());
        this.metadataVersionFromCr = kafkaCr.getSpec().getKafka().getMetadataVersion();
        KafkaConfiguration configuration = new KafkaConfiguration(reconciliation, kafkaCr.getSpec().getKafka().getConfig().entrySet());
        this.interBrokerProtocolVersionFromCr = configuration.getConfigOption(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION);
        this.logMessageFormatVersionFromCr = configuration.getConfigOption(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION);

        // Store operators and Feature Gates configuration
        this.versions = config.versions();
        this.stsOperator = supplier.stsOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.podOperator = supplier.podOperations;
    }

    /**
     * Collects the information from the Kubernetes resources and creates the KafkaVersionChange instance describing the
     * version change in this reconciliation
     *
     * @return  Future which completes with the KafkaVersionChange instance
     */
    public Future<KafkaVersionChange> reconcile()    {
        return getVersionFromController()
                .compose(i -> getPods())
                .compose(this::detectToAndFromVersions)
                .compose(i -> prepareVersionChange());
    }

    /**
     * Collects the information whether the controller resource (StatefulSet or PodSet) exists and what Kafka versions
     * they carry in their annotations.
     *
     * @return  Future which completes when the version is collected from the controller resource
     */
    private Future<Void> getVersionFromController() {
        Future<StatefulSet> stsFuture = stsOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaComponentName(reconciliation.name()));
        Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaComponentName(reconciliation.name()));

        return Future.join(stsFuture, podSetFuture)
                .compose(res -> {
                    StatefulSet sts = res.resultAt(0);
                    StrimziPodSet podSet = res.resultAt(1);

                    if (sts != null && podSet != null)  {
                        // Both StatefulSet and PodSet exist => we use StrimziPodSet as the main controller resource
                        versionFromControllerResource = Annotations.annotations(podSet).get(ANNO_STRIMZI_IO_KAFKA_VERSION);
                        freshDeployment = false;
                    } else if (podSet != null) {
                        // PodSet exists, StatefulSet does not => we create the description from the PodSet
                        versionFromControllerResource = Annotations.annotations(podSet).get(ANNO_STRIMZI_IO_KAFKA_VERSION);
                        freshDeployment = false;
                    } else if (sts != null) {
                        // StatefulSet exists, PodSet does not exist => we create the description from the StatefulSet
                        versionFromControllerResource = Annotations.annotations(sts).get(ANNO_STRIMZI_IO_KAFKA_VERSION);
                        freshDeployment = false;
                    }

                    return Future.succeededFuture();
                });
    }

    /**
     * Collects any existing Kafka pods so that we can later get the version information from them.
     *
     * @return  Future which completes when the Kafka broker pods are retrieved
     */
    private Future<List<Pod>> getPods()   {
        Labels selectorLabels = Labels.forStrimziKind(Kafka.RESOURCE_KIND)
                .withStrimziCluster(reconciliation.name())
                .withStrimziName(KafkaResources.kafkaComponentName(reconciliation.name()));

        return podOperator.listAsync(reconciliation.namespace(), selectorLabels);
    }

    /**
     * Detects the current and desired Kafka versions based on the information collected from Kubernetes
     *
     * @return  Future which completes when the "to" and "from" versions are collected
     */
    private Future<Void> detectToAndFromVersions(List<Pod> pods)    {
        String lowestKafkaVersion = versionFromControllerResource;
        String highestKafkaVersion = versionFromControllerResource;

        for (Pod pod : pods)    {
            // Collect the different annotations from the pods
            String currentVersion = Annotations.stringAnnotation(pod, ANNO_STRIMZI_IO_KAFKA_VERSION, null);
            String currentMessageFormat = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, null);
            String currentIbp = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, null);

            // We find the highest and lowest used Kafka version. This is used to detect any upgrades or
            // downgrades which failed in the middle and continue with them.
            if (currentVersion != null) {
                if (highestKafkaVersion == null)  {
                    highestKafkaVersion = currentVersion;
                } else if (compareDottedVersions(highestKafkaVersion, currentVersion) < 0) {
                    highestKafkaVersion = currentVersion;
                }

                if (lowestKafkaVersion == null)  {
                    lowestKafkaVersion = currentVersion;
                } else if (compareDottedVersions(lowestKafkaVersion, currentVersion) > 0) {
                    lowestKafkaVersion = currentVersion;
                }
            }

            // We find the highest used log.message.format.version. This is later used to validate
            // upgrades or downgrades.
            if (currentMessageFormat != null) {
                if (highestLogMessageFormatVersionFromPods == null)  {
                    highestLogMessageFormatVersionFromPods = currentMessageFormat;
                } else if (compareDottedIVVersions(highestLogMessageFormatVersionFromPods, currentMessageFormat) < 0) {
                    highestLogMessageFormatVersionFromPods = currentMessageFormat;
                }
            }

            // We find the highest used inter.broker.protocol.version. This is later used to validate
            // upgrades or downgrades.
            if (currentIbp != null)  {
                if (highestInterBrokerProtocolVersionFromPods == null)  {
                    highestInterBrokerProtocolVersionFromPods = currentIbp;
                } else if (compareDottedIVVersions(highestInterBrokerProtocolVersionFromPods, currentIbp) < 0) {
                    highestInterBrokerProtocolVersionFromPods = currentIbp;
                }
            }
        }

        // We decide what is the current Kafka version used and create the KafkaVersionChange object
        // describing the situation.
        if (lowestKafkaVersion == null)   {
            if (freshDeployment && pods.isEmpty())  {
                // No version found in Pods or StatefulSet because they do not exist => This means we
                // are dealing with a brand new Kafka cluster or a Kafka cluster with deleted
                // StatefulSet and Pods. New cluster does not need an upgrade. Cluster without
                // StatefulSet / Pods cannot be rolled. So we can just deploy a new one with the desired
                // version. So we can use the desired version and set the version change to noop.
                versionFrom = versionFromCr;
                versionTo = versionFromCr;
            } else {
                // Either Pods or StatefulSet already exist. However, none of them contains the version
                // annotation. This suggests they are not created by the current versions of Strimzi.
                // Without the annotation, we cannot detect the Kafka version and decide on upgrade.
                LOGGER.warnCr(reconciliation, "Kafka Pods or StrimziPodSet exist, but do not contain the {} annotation to detect their version. Kafka upgrade cannot be detected.", ANNO_STRIMZI_IO_KAFKA_VERSION);
                throw new KafkaUpgradeException("Kafka Pods or StrimziPodSet exist, but do not contain the " + ANNO_STRIMZI_IO_KAFKA_VERSION + " annotation to detect their version. Kafka upgrade cannot be detected.");
            }
        } else if (lowestKafkaVersion.equals(highestKafkaVersion)) {
            // All brokers have the same version. We can use it as the current version.
            versionFrom = versions.version(lowestKafkaVersion);
            versionTo = versionFromCr;
        } else if (compareDottedVersions(highestKafkaVersion, versionFromCr.version()) > 0)    {
            // Highest Kafka version used by the brokers is higher than desired => suspected downgrade
            versionFrom = versions.version(highestKafkaVersion);
            versionTo = versionFromCr;
        } else {
            // Highest Kafka version used by the brokers is equal or lower than desired => suspected upgrade
            versionFrom = versions.version(lowestKafkaVersion);
            versionTo = versionFromCr;
        }

        return Future.succeededFuture();
    }

    /**
     * Plans the version change and creates a KafkaVersionChange object which contains the main versions as well as the
     * inter.broker.protocol.version and log.message.format.version.
     *
     * @return  Future with the KafkaVersionChange instance describing the Kafka version changes
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private Future<KafkaVersionChange>  prepareVersionChange()  {
        if (versionFrom.compareTo(versionTo) == 0) { // => no version change
            LOGGER.debugCr(reconciliation, "{}: No Kafka version change", reconciliation);

            if (interBrokerProtocolVersionFromCr == null) {
                // When inter.broker.protocol.version is not set, we set it to current Kafka version
                interBrokerProtocolVersion = versionFromCr.protocolVersion();

                if (highestInterBrokerProtocolVersionFromPods != null
                        && !versionFromCr.protocolVersion().equals(highestInterBrokerProtocolVersionFromPods)) {
                    LOGGER.infoCr(reconciliation, "Upgrading Kafka inter.broker.protocol.version from {} to {}", highestInterBrokerProtocolVersionFromPods, versionFromCr.protocolVersion());

                    if (compareDottedIVVersions(versionFromCr.protocolVersion(), "3.0") >= 0) {
                        // From Kafka 3.0.0, the log.message.format.version is ignored when inter.broker.protocol.version is set to 3.0 or higher
                        // We set the log.message.format.version immediately to the same version as inter.broker.protocol.version to avoid unnecessary rolling update
                        logMessageFormatVersion = versionFromCr.messageVersion();
                    } else if (logMessageFormatVersionFromCr == null
                            && highestLogMessageFormatVersionFromPods != null) {
                        // For Kafka versions older than 3.0.0, inter.broker.protocol.version and log.message.format.version should not change in the same rolling
                        // update. When this rolling update is going to change the inter.broker.protocol.version, we keep the old log.message.format.version
                        logMessageFormatVersion = highestLogMessageFormatVersionFromPods;
                    }
                }
            }

            if (logMessageFormatVersionFromCr == null) {
                if (interBrokerProtocolVersionFromCr != null
                        && compareDottedIVVersions(interBrokerProtocolVersionFromCr, "3.0") >= 0) {
                    // When inter.broker.protocol.version is set to 3.0 or higher, the log.message.format.version is
                    // ignored. To avoid unnecessary rolling updates just because changing log.message.format.version,
                    // when the user does not explicitly set it but sets inter.broker.protocol.version, we mirror
                    // inter.broker.protocol.version for the log.message.format.version as well.
                    logMessageFormatVersion = interBrokerProtocolVersionFromCr;
                } else {
                    // When log.message.format.version is not set, we set it to current Kafka version
                    logMessageFormatVersion = versionFromCr.messageVersion();
                    if (highestLogMessageFormatVersionFromPods != null &&
                            !versionFromCr.messageVersion().equals(highestLogMessageFormatVersionFromPods)) {
                        LOGGER.infoCr(reconciliation, "Upgrading Kafka log.message.format.version from {} to {}", highestLogMessageFormatVersionFromPods, versionFromCr.messageVersion());
                    }
                }
            }
        } else {
            if (versionFrom.compareTo(versionTo) < 0) { // => is upgrade
                LOGGER.infoCr(reconciliation, "Kafka is upgrading from {} to {}", versionFrom.version(), versionTo.version());

                // We make sure that the highest log.message.format.version or inter.broker.protocol.version
                // used by any of the brokers is not higher than the broker version we upgrade from.
                if ((highestLogMessageFormatVersionFromPods != null && compareDottedIVVersions(versionFrom.messageVersion(), highestLogMessageFormatVersionFromPods) < 0)
                        || (highestInterBrokerProtocolVersionFromPods != null && compareDottedIVVersions(versionFrom.protocolVersion(), highestInterBrokerProtocolVersionFromPods) < 0)) {
                    LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used by the brokers have to be lower or equal to the Kafka broker version we upgrade from ({})", highestInterBrokerProtocolVersionFromPods, highestInterBrokerProtocolVersionFromPods, versionFrom.version());
                    throw new KafkaUpgradeException("log.message.format.version (" + highestLogMessageFormatVersionFromPods + ") and inter.broker.protocol.version (" + highestInterBrokerProtocolVersionFromPods + ") used by the brokers have to be lower or equal to the Kafka broker version we upgrade from (" + versionFrom.version() + ")");
                }

                String desiredLogMessageFormat = logMessageFormatVersionFromCr;
                String desiredInterBrokerProtocol = interBrokerProtocolVersionFromCr;

                // The desired log.message.format.version will be configured in the new brokers. So it cannot be higher
                // that the Kafka version we are upgrading from. If it is, we override it with the version we are
                // upgrading from. If it is not set, we set it to the version we are upgrading from.
                if (desiredLogMessageFormat == null
                        || compareDottedIVVersions(versionFrom.messageVersion(), desiredLogMessageFormat) < 0) {
                    logMessageFormatVersion = versionFrom.messageVersion();
                }

                // The desired inter.broker.protocol.version will be configured in the new brokers. So it cannot be
                // higher that the Kafka version we are upgrading from. If it is, we override it with the version we
                // are upgrading from. If it is not set, we set it to the version we are upgrading from.
                if (desiredInterBrokerProtocol == null
                        || compareDottedIVVersions(versionFrom.protocolVersion(), desiredInterBrokerProtocol) < 0) {
                    interBrokerProtocolVersion = versionFrom.protocolVersion();
                }
            } else {
                // Has to be a downgrade
                LOGGER.infoCr(reconciliation, "Kafka is downgrading from {} to {}", versionFrom.version(), versionTo.version());

                // The currently used log.message.format.version and inter.broker.protocol.version cannot be higher
                // than the version we are downgrading to. If it is we fail the reconciliation. If they are not set,
                // we assume that it will use the default value which is the "from" version. In such case we fail the
                // reconciliation as well.
                if (highestLogMessageFormatVersionFromPods == null
                        || compareDottedIVVersions(versionTo.messageVersion(), highestLogMessageFormatVersionFromPods) < 0
                        || highestInterBrokerProtocolVersionFromPods == null
                        || compareDottedIVVersions(versionTo.protocolVersion(), highestInterBrokerProtocolVersionFromPods) < 0) {
                    LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to ({})", highestLogMessageFormatVersionFromPods, highestInterBrokerProtocolVersionFromPods, versionTo.version());
                    throw new KafkaUpgradeException("log.message.format.version (" + highestLogMessageFormatVersionFromPods + ") and inter.broker.protocol.version (" + highestInterBrokerProtocolVersionFromPods + ") used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to (" + versionTo.version() + ")");
                }

                String desiredLogMessageFormat = logMessageFormatVersionFromCr;
                String desiredInterBrokerProtocol = interBrokerProtocolVersionFromCr;

                // If log.message.format.version is not set, we set it to the version we are downgrading to.
                if (desiredLogMessageFormat == null) {
                    desiredLogMessageFormat = versionTo.messageVersion();
                    logMessageFormatVersion = versionTo.messageVersion();
                }

                // If inter.broker.protocol.version is not set, we set it to the version we are downgrading to.
                if (desiredInterBrokerProtocol == null) {
                    desiredInterBrokerProtocol = versionTo.protocolVersion();
                    interBrokerProtocolVersion = versionTo.protocolVersion();
                }

                // Either log.message.format.version or inter.broker.protocol.version are higher than the Kafka
                // version we are downgrading to. This should normally not happen since that should not pass the CR
                // validation. However, we still double-check it as safety.
                if (compareDottedIVVersions(versionTo.messageVersion(), desiredLogMessageFormat) < 0
                        || compareDottedIVVersions(versionTo.protocolVersion(), desiredInterBrokerProtocol) < 0) {
                    LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used in the Kafka CR have to be set and be lower or equal to the Kafka broker version we downgrade to ({})", highestLogMessageFormatVersionFromPods, highestInterBrokerProtocolVersionFromPods, versionTo.version());
                    throw new KafkaUpgradeException("log.message.format.version and inter.broker.protocol.version used in the Kafka CR have to be set and be lower or equal to the Kafka broker version we downgrade to");
                }
            }
        }

        // For migration from ZooKeeper to KRaft, we need to configure the metadata version on the new controller nodes.
        // For that, we need to set the metadata version even in the ZooKeeperVersionChangeCreator class even through it
        // is not used in KRaft mode. As the controllers will be new, we set it based on the Kafka CR or based on the
        // Kafka version used (which will be always the versionTo on the newly deployed controllers).
        return Future.succeededFuture(new KafkaVersionChange(versionFrom, versionTo, interBrokerProtocolVersion, logMessageFormatVersion, metadataVersionFromCr != null ? metadataVersionFromCr : versionTo.metadataVersion()));
    }
}
