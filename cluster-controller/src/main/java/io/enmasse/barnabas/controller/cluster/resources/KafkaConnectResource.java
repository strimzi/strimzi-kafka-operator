package io.enmasse.barnabas.controller.cluster.resources;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConnectResource extends AbstractResource {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnectResource.class.getName());

    private final String name;

    private final int restApiPort = 8083;

    private int replicas = DEFAULT_REPLICAS;
    private String image = "enmasseproject/kafka-connect:latest";
    private String livenessProbePath = "/";
    private int livenessProbeTimeout = 5;
    private int livenessProbeInitialDelay = 60;
    private String readinessProbePath = "/";
    private int readinessProbeTimeout = 5;
    private int readinessProbeInitialDelay = 60;

    private String kafkaBootstrapServers = "kafka:9092";

    private static int DEFAULT_REPLICAS = 3;

    private KafkaConnectResource(String name, String namespace, Vertx vertx, K8SUtils k8s) {
        super(namespace, new ResourceId("kafkaBootstrapServers-connect", name), vertx, k8s);
        this.name = name;
    }

    public static KafkaConnectResource fromConfigMap(ConfigMap cm, Vertx vertx, K8SUtils k8s) {
        KafkaConnectResource kafkaConnect = new KafkaConnectResource(cm.getMetadata().getName(), cm.getMetadata().getNamespace(), vertx, k8s);
        kafkaConnect.setLabels(cm.getMetadata().getLabels());

        if (cm.getData().containsKey("nodes")) {
            kafkaConnect.setReplicas(Integer.parseInt(cm.getData().get("nodes")));
        }

        if (cm.getData().containsKey("kafka-bootstrap-servers")) {
            kafkaConnect.setKafkaBootstrapServers(cm.getData().get("kafka-bootstrap-servers"));
        }

        return kafkaConnect;
    }

    public static KafkaConnectResource fromDeployment(Deployment dep, Vertx vertx, K8SUtils k8s) {
        KafkaConnectResource kafkaConnect =  new KafkaConnectResource(dep.getMetadata().getName(), dep.getMetadata().getNamespace(), vertx, k8s);

        kafkaConnect.setLabels(dep.getMetadata().getLabels());
        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        kafkaConnect.setKafkaBootstrapServers(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().filter(env -> env.getName().equals("KAFKA_CONNECT_BOOTSTRAP_SERVERS")).collect(Collectors.toList()).get(0).getValue());
        return kafkaConnect;
    }

    public void create(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();
                if (!exists()) {
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                log.info("Creating Kafka Connect {}", name);
                                try {
                                    k8s.createService(namespace, generateService());
                                    k8s.createDeployment(namespace, generateDeployment());
                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exceptoion: {}", e.toString());
                                    future.fail(e);
                                }
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Kafka Connect cluster created {}", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to create Kafka Connect cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to create Kafka cluster"));
                                }
                            });
                }
                else {
                    log.info("Kafka Connect cluster {} seems to already exist", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to create Kafka Connect cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka Connect cluster"));
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
                                log.info("Deleting Kafka Connect {}", name);
                                k8s.deleteService(namespace, name);
                                k8s.deleteDeployment(namespace, name);
                                future.complete();
                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Kafka Connect cluster {} delete", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to delete Kafka Connect cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to delete Kafka Connect cluster"));
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
        Deployment dep = k8s.getDeployment(namespace, name);

        if (replicas > dep.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, dep.getSpec().getReplicas());
            diff.setScaleUp(true);
        }
        else if (replicas < dep.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, dep.getSpec().getReplicas());
            diff.setScaleDown(true);
        }

        if (!getLabelsWithName(name).equals(dep.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(name), dep.getMetadata().getLabels());
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
                                log.info("Updating Kafka Connect {}", name);

                                if (diff.getScaleDown() || diff.getScaleUp()) {
                                    log.info("Scaling Kafka Connect to {} replicas", replicas);
                                    k8s.getDeploymentResource(namespace, name).scale(replicas, true);
                                }

                                try {
                                    log.info("Patching deployment");

                                    //k8s.getDeploymentResource(namespace, name).edit().editMetadata().withLabels(getLabelsWithName(name)).endMetadata().done();
                                    k8s.getDeploymentResource(namespace, name).replace(generateDeployment());

                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exceptoion: {}", e.toString());
                                    future.fail(e);
                                }
                                //log.info("Patching stateful set to {} with {} replicas", generateDeployment(), generateDeployment().getSpec().getReplicas());
                                //StatefulSet s = k8s.getStatefulSetResource(namespace, name).edit().editMetadata().withLabels(getLabelsWithName(name)).endMetadata().done();
                                //StatefulSet s = k8s.getKubernetesClient().apps().statefulSets().inNamespace(namespace).createOrReplace(generateDeployment());

                                /*StatefulSet src = k8s.getStatefulSet(namespace, name);
                                src.getMetadata().setLabels(getLabelsWithName(name));*/
                                //log.info("Updating stateful set to {} with {} replicas", src, generateDeployment().getSpec().getReplicas());
                                //StatefulSet s = k8s.getKubernetesClient().apps().statefulSets().inNamespace(namespace).createOrReplace(src);
                                //StatefulSet s = k8s.getStatefulSetResource(namespace, name).edit().editMetadata().withLabels(getLabelsWithName(name)).endMetadata().done();


                                /*for (int i = 0; i < replicas; i++) {
                                    k8s.getKubernetesClient().pods().inNamespace(namespace).withName(name + "-" + Integer.toString(i)).edit().editMetadata().addToLabels(getLabels()).endMetadata().done();
                                }

                                k8s.getStatefulSetResource(namespace, name).edit().editSpec().withSelector(new LabelSelectorBuilder().withMatchLabels(getLabels()).build()).endSpec().done();*/

                                /*StatefulSet src = k8s.getStatefulSet(namespace, name);
                                src.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("enmasseproject/kafkaBootstrapServers-statefulsets:latest");
                                k8s.getStatefulSetResource(namespace, name).replace(src);*/



                                //log.info("Update complete: {}", s);
                                //k8s.getServiceResource(namespace, name).createOrReplace(generateService());
                                //k8s.getServiceResource(namespace, headlessName).createOrReplace(generateHeadlessService());


                            }, false, res2 -> {
                                if (res2.succeeded()) {
                                    log.info("Kafka Connect cluster updated {}", name);
                                    lock.release();
                                    handler.handle(Future.succeededFuture());
                                } else {
                                    log.error("Failed to update Kafka Connect cluster {}", name);
                                    lock.release();
                                    handler.handle(Future.failedFuture("Failed to update Kafka Connect cluster"));
                                }
                            });
                }
                else if (!diff.getDifferent()) {
                    log.info("Kafka Connect cluster {} is up to date", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
                else {
                    log.info("Kafka Connect cluster {} seems to not exist", name);
                    lock.release();
                    handler.handle(Future.succeededFuture());
                }
            } else {
                log.error("Failed to acquire lock to create Kafka Connect cluster {}", name);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka Connect cluster"));
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
                .withPorts(k8s.createServicePort("rest-api", restApiPort, restApiPort))
                .endSpec()
                .build();

        return svc;
    }

    private Deployment generateDeployment() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(new EnvVarBuilder().withName("KAFKA_CONNECT_BOOTSTRAP_SERVERS").withValue(kafkaBootstrapServers).build())
                .withPorts(k8s.createContainerPort("rest-api", restApiPort))
                .withLivenessProbe(k8s.createHttpProbe(livenessProbePath, "rest-api", livenessProbeInitialDelay, livenessProbeTimeout))
                .withReadinessProbe(k8s.createHttpProbe(readinessProbePath, "rest-api", readinessProbeInitialDelay, readinessProbeTimeout))
                .build();

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(getLabels())
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        return dep;
    }

    private String getLockName() {
        return "kafkaBootstrapServers-connect::lock::" + name;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public boolean exists() {
        return k8s.deploymentExists(namespace, name) && k8s.serviceExists(namespace, name);
    }

    public boolean atLeastOneExists() {
        return k8s.deploymentExists(namespace, name) || k8s.serviceExists(namespace, name);
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }
}
