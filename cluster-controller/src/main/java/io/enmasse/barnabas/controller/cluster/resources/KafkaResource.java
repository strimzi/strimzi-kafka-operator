package io.enmasse.barnabas.controller.cluster.resources;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategy;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaResource extends AbstractResource {
    private static final Logger log = LoggerFactory.getLogger(KafkaResource.class.getName());

    //private final KubernetesClient client;
    private final String name;
    //private final String namespace;
    private final String headlessName;
    private final String zookeeper;

    private final int clientPort = 9092;
    private final String mounthPath = "/var/lib/kafka";
    private final String volumeName = "kafka-storage";

    private int replicas = DEFAULT_REPLICAS;
    private String image = "scholzj/kafka-statefulsets:latest";
    private String livenessProbeScript = "/opt/kafka/kafka_healthcheck.sh";
    private int livenessProbeTimeout = 5;
    private int livenessProbeInitialDelay = 15;
    private String readinessProbeScript = "/opt/kafka/kafka_healthcheck.sh";
    private int readinessProbeTimeout = 5;
    private int readinessProbeInitialDelay = 15;

    private static int DEFAULT_REPLICAS = 3;

    private KafkaResource(String name, String namespace, Vertx vertx, K8SUtils k8s) {
        super(namespace, new ResourceId("zookeeper", name), vertx, k8s);
        this.name = name;
        this.headlessName = name + "-headless";
        this.zookeeper = name + "-zookeeper" + ":2181";
        //this.namespace = namespace;
        //this.client = client;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public static KafkaResource fromConfigMap(ConfigMap cm, Vertx vertx, K8SUtils k8s) {
        KafkaResource kafka = new KafkaResource(cm.getMetadata().getName(), cm.getMetadata().getNamespace(), vertx, k8s);
        kafka.setLabels(cm.getMetadata().getLabels());

        if (cm.getData().containsKey("kafka-nodes")) {
            kafka.setReplicas(Integer.parseInt(cm.getData().get("kafka-nodes")));
        }

        return kafka;
    }

    public static KafkaResource fromStatefulSet(StatefulSet ss, Vertx vertx, K8SUtils k8s) {
        KafkaResource kafka =  new KafkaResource(ss.getMetadata().getName(), ss.getMetadata().getNamespace(), vertx, k8s);

        kafka.setLabels(ss.getMetadata().getLabels());
        kafka.setReplicas(ss.getSpec().getReplicas());
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
                                k8s.createService(namespace, generateService());
                                k8s.createService(namespace, generateHeadlessService());
                                k8s.createStatefulSet(namespace, generateStatefulSet());
                                future.complete();
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
                                k8s.deleteService(namespace, name);
                                k8s.deleteStatefulSet(namespace, name);
                                k8s.deleteService(namespace, headlessName);
                                future.complete();
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

        if (!getLabelsWithName(name).equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(name), ss.getMetadata().getLabels());
            diff.setDifferent(true);
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

                                if (diff.getScaleDown() || diff.getScaleUp()) {
                                    log.info("Scaling to {} replicas", replicas);
                                    k8s.getStatefulSetResource(namespace, name).scale(replicas, true);
                                }

                                log.info("Patching stateful set to {} with {} replicas", generateStatefulSet(), generateStatefulSet().getSpec().getReplicas());
                                //StatefulSet s = k8s.getStatefulSetResource(namespace, name).edit().editMetadata().withLabels(getLabelsWithName(name)).endMetadata().done();
                                StatefulSet s = k8s.getKubernetesClient().apps().statefulSets().inNamespace(namespace).createOrReplace(generateStatefulSet());
                                log.info("Patch complete: {}", s);
                                //k8s.getServiceResource(namespace, name).createOrReplace(generateService());
                                //k8s.getServiceResource(namespace, headlessName).createOrReplace(generateHeadlessService());

                                future.complete();
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
                .withLabels(getLabelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .withPorts(k8s.createServicePort("kafka", clientPort, clientPort))
                .endSpec()
                .build();

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
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .withPorts(k8s.createServicePort("kafka", clientPort, clientPort))
                .endSpec()
                .build();

        return svc;
    }

    private StatefulSet generateStatefulSet() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(new EnvVarBuilder().withName("KAFKA_ZOOKEEPER_CONNECT").withValue(zookeeper).build())
                .withVolumeMounts(k8s.createVolumeMount(volumeName, mounthPath))
                .withPorts(k8s.createContainerPort("clientport", clientPort))
                .withLivenessProbe(k8s.createExecProbe(livenessProbeScript, livenessProbeInitialDelay, livenessProbeTimeout))
                .withReadinessProbe(k8s.createExecProbe(readinessProbeScript, readinessProbeInitialDelay, readinessProbeTimeout))
                .build();

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withServiceName(headlessName)
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .withVolumes(k8s.createEmptyDirVolume(volumeName))
                .endSpec()
                .endTemplate()
                .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                .endSpec()
                .build();

        return statefulSet;
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
}
