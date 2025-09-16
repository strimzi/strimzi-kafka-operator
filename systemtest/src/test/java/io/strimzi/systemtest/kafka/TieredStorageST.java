/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.ResourceItem;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.imageBuild.ImageBuild;
import io.strimzi.systemtest.resources.minio.SetupMinio;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.specific.ContainerRuntimeUtils;
import io.strimzi.systemtest.utils.specific.MinioUtils;
import io.strimzi.systemtest.utils.specific.NfsUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestConstants.GLOBAL_POLL_INTERVAL;
import static io.strimzi.systemtest.TestConstants.GLOBAL_TIMEOUT;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.TIERED_STORAGE;
import static io.strimzi.systemtest.utils.specific.NfsUtils.NFS_PVC_NAME;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@MicroShiftNotSupported("We are using Kaniko and OpenShift builds to build Kafka image with TS. To make it working on Microshift we will invest much time with not much additional value.")
@Tag(REGRESSION)
@Tag(TIERED_STORAGE)
@SuiteDoc(
    description = @Desc("This test suite covers scenarios for Tiered Storage integration implemented within Strimzi."),
    beforeTestSteps = {
        @Step(value = "Create test namespace.", expected = "Namespace is created."),
        @Step(value = "Build Kafka image based on passed parameters like image full name, base image, Dockerfile path (via Kaniko or OpenShift build), and include the Aiven Tiered Storage plugin from (<a href=\"https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main\">tiered-storage-for-apache-kafka</a>).", expected = "Kafka image is built with the Aiven Tiered Storage plugin integrated."),
        @Step(value = "Deploy Minio in test namespace and init the client inside the Minio pod.", expected = "Minio is deployed and client is initialized."),
        @Step(value = "Init bucket in Minio for purposes of these tests.", expected = "Bucket is initialized in Minio."),
        @Step(value = "Deploy Cluster Operator.", expected = "Cluster Operator is deployed.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class TieredStorageST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(TieredStorageST.class);

    private static final String IMAGE_NAME = "kafka-tiered-storage";
    private static final String TIERED_STORAGE_DOCKERFILE = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tiered-storage/Dockerfile";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String BUILT_IMAGE_TAG = "latest";
    private static final int SEGMENT_BYTE = 1048576;
    private static final int MESSAGE_COUNT = 10_000;
    private static final String NFS_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/nfs/nfs.yaml";
    private static final String MOUNT_PATH = "/mnt/nfs";
    private static final String VOLUME_NAME = "nfs-volume";
    private TestStorage suiteStorage;
    private String tieredStorageImageName;

    @ParallelTest
    @TestDoc(
        description = @Desc("This testcase is focused on testing of Tiered Storage integration implemented within Strimzi. The tests use the S3 plugin in Aiven Tiered Storage project (<a href=\"https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main\">tiered-storage-for-apache-kafka</a>)."),
        steps = {
            @Step(value = "Deploys KafkaNodePool resource with PV of size 10Gi.", expected = "KafkaNodePool resource is deployed successfully with specified configuration."),
            @Step(value = "Deploy Kafka CustomResource with Tiered Storage configuration pointing to Minio S3, using a built Kafka image. Reduce the `remote.log.manager.task.interval.ms` and `log.retention.check.interval.ms` to minimize delays during log uploads and deletions.", expected = "Kafka CustomResource is deployed successfully with optimized intervals to speed up log uploads and local log deletions."),
            @Step(value = "Creates topic with enabled Tiered Storage sync with size of segments set to 10mb (this is needed to speed up the sync).", expected = "Topic is created successfully with Tiered Storage enabled and segment size of 10mb."),
            @Step(value = "Starts continuous producer to send data to Kafka.", expected = "Continuous producer starts sending data to Kafka."),
            @Step(value = "Wait until Minio size is not empty (contains data from Kafka).", expected = "Minio contains data from Kafka."),
            @Step(value = "Wait until the earliest-local offset to be higher than 0.", expected = "The log segments uploaded to Minio are deleted locally."),
            @Step(value = "Starts a consumer to consume all the produced messages, some of the messages should be located in Minio.", expected = "Consumer can consume all the messages successfully."),
            @Step(value = "Alter the topic config to retention.ms=10 sec to test the remote log deletion.", expected = "The topic config is altered successfully."),
            @Step(value = "Wait until Minio size is 0.", expected = "The data in Minio are deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testTieredStorageWithAivenS3Plugin() {
        deployMinioInstance();

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("10Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(suiteStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withImage(tieredStorageImageName)
                    .withNewTieredStorageCustomTiered()
                        .withNewRemoteStorageManager()
                            .withClassName("io.aiven.kafka.tieredstorage.RemoteStorageManager")
                            .withClassPath(Environment.KAFKA_TIERED_STORAGE_CLASSPATH)
                            .addToConfig("storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
                            .addToConfig("chunk.size", "4194304")
                            // s3 config
                            .addToConfig("storage.s3.endpoint.url",
                                    "http://" + SetupMinio.MINIO + "." + suiteStorage.getNamespaceName() + ".svc.cluster.local:" + SetupMinio.MINIO_PORT)
                            .addToConfig("storage.s3.bucket.name", BUCKET_NAME)
                            .addToConfig("storage.s3.region", "us-east-1")
                            .addToConfig("storage.s3.path.style.access.enabled", "true")
                            .addToConfig("storage.aws.access.key.id", SetupMinio.ADMIN_CREDS)
                            .addToConfig("storage.aws.secret.access.key", SetupMinio.ADMIN_CREDS)
                        .endRemoteStorageManager()
                    .endTieredStorageCustomTiered()
                    // reduce the interval to speed up the test
                    .addToConfig("remote.log.manager.task.interval.ms", 5000)
                    .addToConfig("log.retention.check.interval.ms", 5000)
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(suiteStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName())
            .editSpec()
                .addToConfig("file.delete.delay.ms", 1000)
                .addToConfig("local.retention.ms", 1000)
                // Allow tiered storage sync
                .addToConfig("remote.storage.enable", true)
                // Bytes retention set to 1024mb
                .addToConfig("retention.bytes", 1073741824)
                .addToConfig("retention.ms", 86400000)
                // Segment size is set to 10mb to make it quicker to sync data to Minio
                .addToConfig("segment.bytes", SEGMENT_BYTE)
            .endSpec()
            .build());

        final KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(1)
            .withMessage(String.join("", Collections.nCopies(300, "#")))
            .build();

        KubeResourceManager.get().createResourceWithWait(clients.producerStrimzi());

        MinioUtils.waitForDataInMinio(suiteStorage.getNamespaceName(), BUCKET_NAME);

        // Create admin-client to check offsets
        KubeResourceManager.get().createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );
        waitForEarliestLocalOffsetGreaterThanZero(testStorage.getNamespaceName(), testStorage.getAdminName(), testStorage.getTopicName());

        KubeResourceManager.get().createResourceWithWait(clients.consumerStrimzi());
        // Verify we can consume messages from (a) remote storage and (b) local storage. Because we have verified earlier
        // that the log segments are moved to remote storage (by Minio size check) and deleted locally (by earliest-local offset check),
        // we can verify (a) and (b) by checking if we can consume all messages successfully.
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), MESSAGE_COUNT);

        // Delete data
        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().getConfig().put("retention.ms", 10000)
        );

        MinioUtils.waitForNoDataInMinio(suiteStorage.getNamespaceName(), BUCKET_NAME);
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("This testcase is focused on testing of Tiered Storage integration implemented within Strimzi. The tests use the FileSystem plugin in Aiven Tiered Storage project (<a href=\"https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main\">tiered-storage-for-apache-kafka</a>)."),
        steps = {
            @Step(value = "Deploys KafkaNodePool resource with PV of size 10Gi.", expected = "KafkaNodePool resource is deployed successfully with specified configuration."),
            @Step(value = "Deploys a NFS instance with RoleBinding, serviceAccount, service, StorageClass... related resources.", expected = "NFS resources are deployed successfully."),
            @Step(value = "Deploy Kafka CustomResource with additional NFS volume mounted and Tiered Storage configuration pointing to NFS path, using a built Kafka image. " +
                    "Reduce the `remote.log.manager.task.interval.ms` and `log.retention.check.interval.ms` to minimize delays during log uploads and deletions.", expected = "Kafka CustomResource is deployed successfully with optimized intervals to speed up log uploads and local log deletions."),
            @Step(value = "Creates topic with enabled Tiered Storage sync with size of segments set to 10mb (this is needed to speed up the sync).", expected = "Topic is created successfully with Tiered Storage enabled and segment size of 10mb."),
            @Step(value = "Starts continuous producer to send data to Kafka.", expected = "Continuous producer starts sending data to Kafka."),
            @Step(value = "Wait until the NFS size is greater than one log segment size (contains data from Kafka).", expected = "The NFS contains at least one log segment from Kafka."),
            @Step(value = "Wait until the earliest-local offset to be higher than 0.", expected = "The log segments uploaded to NFS are deleted locally."),
            @Step(value = "Starts a consumer to consume all the produced messages, some of the messages should be located in NFS.", expected = "Consumer can consume all the messages successfully."),
            @Step(value = "Alter the topic config to retention.ms=10 sec to test the remote log deletion.", expected = "The topic config is altered successfully."),
            @Step(value = "Wait until the NFS data is deleted.", expected = "The data in the NFS data is deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testTieredStorageWithAivenFileSystemPlugin() {
        deployNfsInstance();

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("10Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        VolumeMount[] volumeMounts = new VolumeMount[]{
            new VolumeMountBuilder()
                .withName(VOLUME_NAME)
                .withMountPath(MOUNT_PATH)
                .build()
        };
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(suiteStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withImage(tieredStorageImageName)
                    .withNewTieredStorageCustomTiered()
                        .withNewRemoteStorageManager()
                            .withClassName("io.aiven.kafka.tieredstorage.RemoteStorageManager")
                            .withClassPath(Environment.KAFKA_TIERED_STORAGE_CLASSPATH)
                            .addToConfig("storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage")
                            .addToConfig("storage.root", MOUNT_PATH)
                            .addToConfig("chunk.size", "4194304")
                        .endRemoteStorageManager()
                    .endTieredStorageCustomTiered()
                    // mount additional NFS volume in kafka pod
                    .withNewTemplate()
                        .withNewPod()
                            .addNewVolume()
                                .withName(VOLUME_NAME)
                                .withNewPersistentVolumeClaim(NFS_PVC_NAME, false)
                            .endVolume()
                        .endPod()
                        .withNewKafkaContainer()
                            .addToVolumeMounts(volumeMounts)
                        .endKafkaContainer()
                    .endTemplate()
                    // reduce the interval to speed up the test
                    .addToConfig("remote.log.manager.task.interval.ms", 5000)
                    .addToConfig("log.retention.check.interval.ms", 5000)
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(suiteStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName())
            .editSpec()
                .addToConfig("file.delete.delay.ms", 1000)
                .addToConfig("local.retention.ms", 1000)
                // Allow tiered storage sync
                .addToConfig("remote.storage.enable", true)
                // Bytes retention set to 1024mb
                .addToConfig("retention.bytes", 1073741824)
                .addToConfig("retention.ms", 86400000)
                // Segment size is set to 10mb to make it quicker to sync data to NFS
                .addToConfig("segment.bytes", SEGMENT_BYTE)
            .endSpec()
            .build());

        final KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(1)
            .withMessage(String.join("", Collections.nCopies(300, "#")))
            .build();

        KubeResourceManager.get().createResourceWithWait(clients.producerStrimzi());

        // wait for logs uploaded to NFS
        NfsUtils.waitForSizeInNfs(testStorage.getNamespaceName(), size -> size > SEGMENT_BYTE);

        // Create admin-client to check offsets
        KubeResourceManager.get().createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );

        waitForEarliestLocalOffsetGreaterThanZero(testStorage.getNamespaceName(), testStorage.getAdminName(), testStorage.getTopicName());

        KubeResourceManager.get().createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), MESSAGE_COUNT);

        // Delete data
        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().getConfig().put("retention.ms", 10000)
        );

        // wait for remote data deletion
        NfsUtils.waitForSizeInNfs(testStorage.getNamespaceName(), size -> size < SEGMENT_BYTE);
    }

    private void waitForEarliestLocalOffsetGreaterThanZero(String namespace, String adminName, String topicName) {
        final AdminClient adminClient = AdminClientUtils.getConfiguredAdminClient(namespace, adminName);

        TestUtils.waitFor("earliest-local offset to be higher than 0",
            TestConstants.GLOBAL_POLL_INTERVAL_5_SECS, TestConstants.GLOBAL_TIMEOUT_LONG,
            () -> {
                // Fetch earliest-local offsets
                // Check that data are not present locally, earliest-local offset should be higher than 0
                String offsetData = adminClient.fetchOffsets(topicName, String.valueOf(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP));
                long earliestLocalOffset = 0;
                try {
                    earliestLocalOffset = AdminClientUtils.getPartitionsOffset(offsetData, "0");
                    LOGGER.info("earliest-local offset for topic {} is {}", topicName, earliestLocalOffset);
                } catch (JsonProcessingException e) {
                    return false;
                }
                return earliestLocalOffset > 0;
            });
    }

    /**
     * Install NFS instance
     */
    private void deployNfsInstance() {
        LOGGER.info("=== Deploying NFS instance ===");

        // allow NetworkPolicies for the NFS in case that we have "default to deny all" mode enabled
        NetworkPolicyUtils.allowNetworkPolicyAllIngressForMatchingLabel(suiteStorage.getNamespaceName(), "nfs", Map.of(TestConstants.APP_POD_LABEL, "nfs-server-provisioner"));

        String instanceYamlContent = ReadWriteUtils.readFile(NFS_INSTANCE_PATH).replace("NAMESPACE_TO_BE_CHANGE", suiteStorage.getNamespaceName());

        TestUtils.waitFor("NFS Instance deploy", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT, () -> {
            try {
                LOGGER.info("Creating NFS Instance from {}", NFS_INSTANCE_PATH);
                KubeResourceManager.get().kubeCmdClient().inNamespace(suiteStorage.getNamespaceName()).applyContent(instanceYamlContent);
                return true;
            } catch (Exception e) {
                LOGGER.error("Following exception has been thrown during NFS Instance Deployment: {}", e.getMessage());
                return false;
            } finally {
                KubeResourceManager.get().pushToStack(new ResourceItem<>(() ->  {
                    // Add sleep before delete nfs provisioner for gracefully delete Kafka and KafkaNodePool
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(90));
                    KubeResourceManager.get().kubeCmdClient().inNamespace(suiteStorage.getNamespaceName()).deleteContent(instanceYamlContent);
                }));
            }
        });
        StatefulSetUtils.waitForAllStatefulSetPodsReady(suiteStorage.getNamespaceName(), "test-nfs-server-provisioner", 1);
    }

    /**
     * Install Minio instance
     */
    private void deployMinioInstance() {
        SetupMinio.deployMinio(suiteStorage.getNamespaceName());
        SetupMinio.createBucket(suiteStorage.getNamespaceName(), BUCKET_NAME);
    }

    /**
     * Method that builds Kafka image with TieredStorage plugin (Aiven one) in case that {@link Environment#KAFKA_TIERED_STORAGE_IMAGE} is not set.
     * The image is built and then value for the particular image output registry is assigned to {@link #tieredStorageImageName}.
     * Otherwise, the {@link Environment#KAFKA_TIERED_STORAGE_IMAGE} is used and assigned into {@link #tieredStorageImageName}.
     *
     * @throws IOException  IO exception during image build.
     */
    private void resolveTieredStorageImage() throws IOException {
        if (Environment.KAFKA_TIERED_STORAGE_IMAGE.isEmpty()) {
            ImageBuild.buildImage(suiteStorage.getNamespaceName(), IMAGE_NAME, TIERED_STORAGE_DOCKERFILE, BUILT_IMAGE_TAG, Environment.KAFKA_TIERED_STORAGE_BASE_IMAGE);
            tieredStorageImageName = Environment.getImageOutputRegistry(suiteStorage.getNamespaceName(), IMAGE_NAME, BUILT_IMAGE_TAG);
        } else {
            tieredStorageImageName = Environment.KAFKA_TIERED_STORAGE_IMAGE;
        }
    }

    @BeforeAll
    void setup() throws IOException {
        // we skip test case for kind + podman
        assumeFalse(cluster.isKind() && ContainerRuntimeUtils.getRuntime().equals(TestConstants.PODMAN));

        // for RBAC, we are creating everything in co-namespace
        // in order to not delete the Namespace (as in the install() method) we need to install CO first and then
        // do everything else.
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        suiteStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        resolveTieredStorageImage();
    }
}
