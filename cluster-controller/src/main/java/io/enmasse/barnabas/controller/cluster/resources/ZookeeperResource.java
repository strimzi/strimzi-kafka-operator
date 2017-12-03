package io.enmasse.barnabas.controller.cluster.resources;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ZookeeperResource extends AbstractResource {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperResource.class.getName());

    /*private final KubernetesClient client;
    private final Vertx vertx;*/

    private final String name;
    //private final String namespace;
    private final String headlessName;

    private final int clientPort = 2181;
    private final int clusteringPort = 2888;
    private final int leaderElectionPort = 3888;
    private final String mounthPath = "/var/lib/zookeeper";
    private final String volumeName = "zookeeper-storage";

    private int replicas = DEFAULT_REPLICAS;
    private String image = "enmasseproject/zookeeper:latest";
    private String livenessProbeScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int livenessProbeTimeout = 5;
    private int livenessProbeInitialDelay = 15;
    private String readinessProbeScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int readinessProbeTimeout = 5;
    private int readinessProbeInitialDelay = 15;

    private static int DEFAULT_REPLICAS = 3;

    private ZookeeperResource(String name, String namespace, Vertx vertx, K8SUtils k8s) {
        super(namespace, new ResourceId("zookeeper", name), vertx, k8s);
        //this.vertx = vertx;
        this.name = name;
        this.headlessName = name + "-headless";
        //this.namespace = namespace;
        //this.client = client;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> newLabels) {
        this.labels = new HashMap<String, String>(newLabels);

        if (labels.containsKey("kind") && labels.get("kind").equals("kafka")) {
            labels.put("kind", "zookeeper");
        }

        this.labels = labels;
    }

    public static ZookeeperResource fromConfigMap(ConfigMap cm, Vertx vertx, K8SUtils k8s) {
        String name = cm.getMetadata().getName() + "-zookeeper";
        ZookeeperResource zk = new ZookeeperResource(name, cm.getMetadata().getNamespace(), vertx, k8s);

        zk.setLabels(cm.getMetadata().getLabels());

        if (cm.getData().containsKey("zookeeper-nodes")) {
            zk.setReplicas(Integer.parseInt(cm.getData().get("zookeeper-nodes")));
        }

        return zk;
    }

    public static ZookeeperResource fromStatefulSet(StatefulSet ss, Vertx vertx, K8SUtils k8s) {
        String name = ss.getMetadata().getName() + "-zookeeper";
        ZookeeperResource zk =  new ZookeeperResource(name, ss.getMetadata().getNamespace(), vertx, k8s);

        zk.setLabels(ss.getMetadata().getLabels());
        zk.setReplicas(ss.getSpec().getReplicas());

        return zk;
    }

    public void create(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();
                if (!exists()) {
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                log.info("Creating Zookeeper {}", name);
                                k8s.createService(namespace, generateService());
                                k8s.createService(namespace, generateHeadlessService());
                                k8s.createStatefulSet(namespace, generateStatefulSet());
                                future.complete();
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Zookeeper cluster created {}", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to create Zookeeper cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to create Zookeeper cluster"));
                                }
                            });
                }
                else {
                    log.info("Zookeeper cluster {} seems to already exist", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to create Zookeeper cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Zookeeper cluster"));
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
                                log.info("Deleting Zookeeper {}", name);
                                k8s.deleteService(namespace, name);
                                k8s.deleteStatefulSet(namespace, name);
                                k8s.deleteService(namespace, headlessName);
                                future.complete();
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Zookeeper cluster {} delete", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to delete Zookeeper cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to delete Zookeeper cluster"));
                                }
                            });
                }
                else {
                    log.info("Zookeeper cluster {} seems to not exist anymore", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to delete Zookeeper cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to delete Zookeeper cluster"));
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
                .withPorts(k8s.createServicePort("clientport", clientPort, clientPort))
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
                .withPorts(k8s.createServicePort("clientport", clientPort, clientPort))
                .withPorts(k8s.createServicePort("clustering", clusteringPort, clusteringPort))
                .withPorts(k8s.createServicePort("leaderelection", leaderElectionPort, leaderElectionPort))
                .endSpec()
                .build();

        return svc;
    }

    private StatefulSet generateStatefulSet() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(new EnvVarBuilder().withName("ZOOKEEPER_NODE_COUNT").withValue(Integer.toString(replicas)).build())
                .withVolumeMounts(k8s.createVolumeMount(volumeName, mounthPath))
                .withPorts(k8s.createContainerPort("clientport", clientPort),
                        k8s.createContainerPort("clustering", clusteringPort),
                        k8s.createContainerPort("leaderelection", leaderElectionPort))
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
                .endSpec()
                .build();

        return statefulSet;
    }

    private String getLockName() {
        return "zookeeper::lock::" + name;
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
