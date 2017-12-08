package io.enmasse.barnabas.controller.cluster.resources;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaResource extends AbstractResource {
    private static final Logger log = LoggerFactory.getLogger(KafkaResource.class.getName());

    private final String headlessName;

    private final int clientPort = 9092;
    private final String clientPortName = "clients";
    private final String mounthPath = "/var/lib/kafka";
    private final String volumeName = "kafka-storage";

    // Number of replicas
    private int replicas = DEFAULT_REPLICAS;

    // Docker image configuration
    private String image = DEFAULT_IMAGE;

    private String healthCheckScript = "/opt/kafka/kafka_healthcheck.sh";
    private int healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
    private int healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private int defaultReplicationFactor = DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR;
    private int offsetsTopicReplicationFactor = DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
    private int transactionStateLogReplicationFactor = DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;

    // Configuration defaults
    private static String DEFAULT_IMAGE = "enmasseproject/kafka-statefulsets:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Kafka configuration defaults
    private static String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "zookeeper:2181";
    private static int DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR = 3;
    private static int DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = 3;
    private static int DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = 3;

    // Configuration keys
    private static String KEY_IMAGE = "kafka-image";
    private static String KEY_REPLICAS = "kafka-nodes";
    private static String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";

    // Kafka configuration keys
    private static String KEY_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static String KEY_KAFKA_DEFAULT_REPLICATION_FACTOR = "KAFKA_DEFAULT_REPLICATION_FACTOR";
    private static String KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    private static String KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";

    private KafkaResource(String name, String namespace, Vertx vertx, K8SUtils k8s) {
        super(namespace, name, new ResourceId("kafka", name), vertx, k8s);
        this.headlessName = name + "-headless";
    }

    public static KafkaResource fromConfigMap(ConfigMap cm, Vertx vertx, K8SUtils k8s) {
        KafkaResource kafka = new KafkaResource(cm.getMetadata().getName(), cm.getMetadata().getNamespace(), vertx, k8s);
        kafka.setLabels(cm.getMetadata().getLabels());

        kafka.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafka.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafka.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafka.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafka.setZookeeperConnect(cm.getData().getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, cm.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        return kafka;
    }

    public static KafkaResource fromStatefulSet(StatefulSet ss, Vertx vertx, K8SUtils k8s) {
        KafkaResource kafka =  new KafkaResource(ss.getMetadata().getName(), ss.getMetadata().getNamespace(), vertx, k8s);

        kafka.setLabels(ss.getMetadata().getLabels());
        kafka.setReplicas(ss.getSpec().getReplicas());
        kafka.setImage(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        kafka.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        kafka.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        kafka.setZookeeperConnect(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, ss.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        return kafka;
    }

    public void create(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();
                if (!exists()) {
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                log.info("Creating Kafka {}", name);
                                try {
                                    k8s.createService(namespace, generateService());
                                    k8s.createService(namespace, generateHeadlessService());
                                    k8s.createStatefulSet(namespace, generateStatefulSet());
                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exceptoion: {}", e.toString());
                                    future.fail(e);
                                }
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Kafka cluster created {}", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to create Kafka cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to create Kafka cluster"));
                                }
                            });
                }
                else {
                    log.info("Kafka cluster {} seems to already exist", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to create Kafka cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka cluster"));
            }
        });
    }

    public void delete(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();
                if (atLeastOneExists()) {
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                log.info("Deleting Kafka {}", name);
                                try {
                                    k8s.deleteService(namespace, name);
                                    k8s.deleteStatefulSet(namespace, name);
                                    k8s.deleteService(namespace, headlessName);
                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exceptoion: {}", e.toString());
                                    future.fail(e);
                                }
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Kafka cluster {} delete", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to delete Kafka cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to delete Kafka cluster"));
                                }
                            });
                }
                else {
                    log.info("Kafka cluster {} seems to not exist anymore", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to delete Kafka cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to delete Kafka cluster"));
            }
        });
    }

    public ResourceDiffResult diff()  {
        ResourceDiffResult diff = new ResourceDiffResult();
        StatefulSet ss = k8s.getStatefulSet(namespace, name);

        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleUp(true);
        }
        else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleDown(true);
        }

        if (!getLabelsWithName().equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), ss.getMetadata().getLabels());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (!image.equals(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (!zookeeperConnect.equals(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, DEFAULT_KAFKA_ZOOKEEPER_CONNECT))
                || defaultReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR)))
                || offsetsTopicReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR)))
                || transactionStateLogReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR)))
                ) {
            log.info("Diff: Kafka options changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (healthCheckInitialDelay != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka healthcheck timing changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        return diff;
    }

    public void update(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();
                ResourceDiffResult diff = diff();
                if (exists() && diff.getDifferent()) {
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                log.info("Updating Kafka {}", name);

                                try {
                                    if (diff.getScaleDown()) {
                                        log.info("Scaling down to {} replicas", replicas);

                                        int actualReplicas = k8s.getStatefulSet(namespace, name).getSpec().getReplicas();
                                        while (actualReplicas > replicas) {
                                            actualReplicas--;
                                            log.info("Scaling down from {} to {}", actualReplicas+1, actualReplicas);
                                            k8s.getStatefulSetResource(namespace, name).scale(actualReplicas, true);
                                        }

                                        log.info("Scaling down complete");
                                    }

                                    k8s.getStatefulSetResource(namespace, name).cascading(false).patch(patchStatefulSet(k8s.getStatefulSet(namespace, name)));
                                    k8s.getServiceResource(namespace, name).replace(patchService(k8s.getService(namespace, name)));
                                    k8s.getServiceResource(namespace, headlessName).replace(patchHeadlessService(k8s.getService(namespace, headlessName)));

                                    if (diff.getRollingUpdate()) {
                                        log.info("Doing rolling update");
                                        for (int i = 0; i < k8s.getStatefulSet(namespace, name).getSpec().getReplicas(); i++) {
                                            String podName = name + "-" + i;
                                            log.info("Rolling pod {}", podName);
                                            Future deleted = Future.future();
                                            Watcher<Pod> watcher = new RollingUpdateWatcher<Pod>(deleted);

                                            Watch watch = k8s.getKubernetesClient().pods().inNamespace(namespace).withName(podName).watch(watcher);
                                            k8s.getKubernetesClient().pods().inNamespace(namespace).withName(podName).delete();

                                            while (!deleted.isComplete()) {
                                                log.info("Waiting for pod {} to be deleted", podName);
                                                Thread.sleep(1000);
                                            }

                                            watch.close();

                                            while (!k8s.getKubernetesClient().pods().inNamespace(namespace).withName(podName).isReady()) {
                                                log.info("Waiting for pod {} to get ready", podName);
                                                Thread.sleep(1000);
                                            };

                                            log.info("Pod {} rolling update complete", podName);
                                        }
                                        log.info("Rolling update complete");
                                    }

                                    if (diff.getScaleUp()) {
                                        log.info("Scaling up to {} replicas", replicas);
                                        k8s.getStatefulSetResource(namespace, name).scale(replicas, true);
                                    }

                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exception: {}", e.toString());
                                    future.fail(e);
                                }
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Kafka cluster updated {}", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to update Kafka cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to update Kafka cluster"));
                                }
                            });
                }
                else if (!diff.getDifferent()) {
                    log.info("Kafka cluster {} is up to date", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
                else {
                    log.info("Kafka cluster {} seems to not exist", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to create Kafka cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka cluster"));
            }
        });
    }

    private Service generateService() {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(getLabelsWithName())
                .withPorts(k8s.createServicePort(clientPortName, clientPort, clientPort))
                .endSpec()
                .build();

        return svc;
    }

    private Service patchService(Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName());
        svc.getSpec().setSelector(getLabelsWithName());

        return svc;
    }

    private Service generateHeadlessService() {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(headlessName)
                .withLabels(getLabelsWithName(headlessName))
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withClusterIP("None")
                .withSelector(getLabelsWithName())
                .withPorts(k8s.createServicePort(clientPortName, clientPort, clientPort))
                .endSpec()
                .build();

        return svc;
    }

    private Service patchHeadlessService(Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName(headlessName));
        svc.getSpec().setSelector(getLabelsWithName());

        return svc;
    }

    private StatefulSet generateStatefulSet() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(getEnvList())
                .withVolumeMounts(k8s.createVolumeMount(volumeName, mounthPath))
                .withPorts(k8s.createContainerPort(clientPortName, clientPort))
                .withLivenessProbe(getHealthCheck())
                .withReadinessProbe(getHealthCheck())
                .build();

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .endMetadata()
                .withNewSpec()
                .withPodManagementPolicy("Parallel")
                .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                .withSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build())
                .withServiceName(headlessName)
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .withVolumes(k8s.createEmptyDirVolume(volumeName))
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        return statefulSet;
    }

    private StatefulSet patchStatefulSet(StatefulSet statefulSet) {
        statefulSet.getMetadata().setLabels(getLabelsWithName());
        statefulSet.getSpec().setSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build());
        statefulSet.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(image);
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setLivenessProbe(getHealthCheck());
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setReadinessProbe(getHealthCheck());
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(getEnvList());

        return statefulSet;
    }

    private List<EnvVar> getEnvList() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_ZOOKEEPER_CONNECT).withValue(zookeeperConnect).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR).withValue(String.valueOf(defaultReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR).withValue(String.valueOf(offsetsTopicReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR).withValue(String.valueOf(transactionStateLogReplicationFactor)).build());

        return varList;
    }

    private Probe getHealthCheck() {
        return k8s.createExecProbe(healthCheckScript, healthCheckInitialDelay, healthCheckTimeout);
    }

    private String getLockName() {
        return "kafka::lock::" + name;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public boolean exists() {
        return k8s.statefulSetExists(namespace, name) && k8s.serviceExists(namespace, name) && k8s.serviceExists(namespace, headlessName);
    }

    public boolean atLeastOneExists() {
        return k8s.statefulSetExists(namespace, name) || k8s.serviceExists(namespace, name) || k8s.serviceExists(namespace, headlessName);
    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setHealthCheckTimeout(int healthCheckTimeout) {
        this.healthCheckTimeout = healthCheckTimeout;
    }

    public void setHealthCheckInitialDelay(int healthCheckInitialDelay) {
        this.healthCheckInitialDelay = healthCheckInitialDelay;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public void setDefaultReplicationFactor(int defaultReplicationFactor) {
        this.defaultReplicationFactor = defaultReplicationFactor;
    }

    public void setOffsetsTopicReplicationFactor(int offsetsTopicReplicationFactor) {
        this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor;
    }

    public void setTransactionStateLogReplicationFactor(int transactionStateLogReplicationFactor) {
        this.transactionStateLogReplicationFactor = transactionStateLogReplicationFactor;
    }
}

class RollingUpdateWatcher<T> implements Watcher<T> {
    private static final Logger log = LoggerFactory.getLogger(RollingUpdateWatcher.class.getName());
    private final Future deleted;

    public RollingUpdateWatcher(Future deleted) {
        this.deleted = deleted;
    }

    @Override
    public void eventReceived(Action action, T pod) {
        switch (action) {
            case DELETED:
                log.info("Pod has been deleted");
                deleted.complete();
                break;
            case ADDED:
            case MODIFIED:
                log.info("Ignored action {} while waiting for Pod deletion", action);
                break;
            case ERROR:
                log.error("Error while waiting for Pod deletion");
                break;
            default:
                log.error("Unknown action {} while waiting for pod deletion", action);
        }
    }

    @Override
    public void onClose(KubernetesClientException e) {
        if (e != null && !deleted.isComplete()) {
            log.error("Kubernetes watcher has been closed with exception!", e);
            deleted.fail(e);
        }
        else {
            log.info("Kubernetes watcher has been closed!");
        }
    }
}