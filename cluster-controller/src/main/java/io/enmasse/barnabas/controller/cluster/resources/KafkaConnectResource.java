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

import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConnectResource extends AbstractResource {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnectResource.class.getName());

    // Port configuration
    private final int restApiPort = 8083;
    private final String restApiPortName = "rest-api";

    // Number of replicas
    private int replicas = DEFAULT_REPLICAS;

    // Docker image configuration
    private String image = DEFAULT_IMAGE;

    // Probe configuration
    private String healthCheckPath = "/";
    private int healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
    private int healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

    // Kafka Connect configuration
    private String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private String groupId = DEFAULT_GROUP_ID;
    private String keyConverter = DEFAULT_KEY_CONVERTER;
    private Boolean keyConverterSchemasEnable = DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE;
    private String valueConverter = DEFAULT_VALUE_CONVERTER;
    private Boolean valueConverterSchemasEnable = DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE;
    private int configStorageReplicationFactor = DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR;
    private int offsetStorageReplicationFactor = DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR;
    private int statusStorageReplicationFactor = DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR;

    // Configuration defaults
    private static String DEFAULT_IMAGE = "enmasseproject/kafka-connect:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 60;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Kafka Connect configuration defaults
    private static String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";
    private static String DEFAULT_GROUP_ID = "connect-cluster";
    private static String DEFAULT_KEY_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static Boolean DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE = true;
    private static String DEFAULT_VALUE_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static Boolean DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE = true;
    private static int DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR = 3;
    private static int DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR = 3;
    private static int DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR = 3;

    // Configuration keys
    private static String KEY_IMAGE = "image";
    private static String KEY_REPLICAS = "nodes";
    private static String KEY_HEALTHCHECK_DELAY = "healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "healthcheck-timeout";

    // Kafka Connect configuration keys
    private static String KEY_BOOTSTRAP_SERVERS = "KAFKA_CONNECT_BOOTSTRAP_SERVERS";
    private static String KEY_GROUP_ID = "KAFKA_CONNECT_GROUP_ID";
    private static String KEY_KEY_CONVERTER = "KAFKA_CONNECT_KEY_CONVERTER";
    private static String KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE = "KAFKA_CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE";
    private static String KEY_VALUE_CONVERTER = "KAFKA_CONNECT_VALUE_CONVERTER";
    private static String KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE = "KAFKA_CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE";
    private static String KEY_CONFIG_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR";
    private static String KEY_OFFSET_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR";
    private static String KEY_STATUS_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR";

    private KafkaConnectResource(String name, String namespace, Vertx vertx, K8SUtils k8s) {
        super(namespace, name, new ResourceId("bootstrapServers-connect", name), vertx, k8s);
    }

    public static KafkaConnectResource fromConfigMap(ConfigMap cm, Vertx vertx, K8SUtils k8s) {
        KafkaConnectResource kafkaConnect = new KafkaConnectResource(cm.getMetadata().getName(), cm.getMetadata().getNamespace(), vertx, k8s);
        kafkaConnect.setLabels(cm.getMetadata().getLabels());

        kafkaConnect.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafkaConnect.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafkaConnect.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafkaConnect.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafkaConnect.setBootstrapServers(cm.getData().getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(cm.getData().getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(cm.getData().getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(cm.getData().getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(cm.getData().getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(cm.getData().getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        return kafkaConnect;
    }

    // Constructing KafkaConnect from Deployment should be used only to delete the deployment
    public static KafkaConnectResource fromDeployment(Deployment dep, Vertx vertx, K8SUtils k8s) {
        KafkaConnectResource kafkaConnect =  new KafkaConnectResource(dep.getMetadata().getName(), dep.getMetadata().getNamespace(), vertx, k8s);

        kafkaConnect.setLabels(dep.getMetadata().getLabels());
        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        kafkaConnect.setImage(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        kafkaConnect.setHealthCheckInitialDelay(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        kafkaConnect.setHealthCheckInitialDelay(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        kafkaConnect.setBootstrapServers(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

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
                                try {
                                    k8s.deleteService(namespace, name);
                                    k8s.deleteDeployment(namespace, name);
                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exceptoion: {}", e.toString());
                                    future.fail(e);
                                }
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

        if (!getLabelsWithName().equals(dep.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), dep.getMetadata().getLabels());
            diff.setDifferent(true);
        }

        if (!image.equals(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (!bootstrapServers.equals(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS))
                || !groupId.equals(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID))
                || !keyConverter.equals(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER))
                || keyConverterSchemasEnable != Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE)))
                || !valueConverter.equals(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER))
                || keyConverterSchemasEnable != Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE)))
                || configStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR)))
                || offsetStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR)))
                || statusStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR)))
                ) {
            log.info("Diff: Kafka Connect options changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (healthCheckInitialDelay != dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka Connect healthcheck timing changed");
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
                                log.info("Updating Kafka Connect {}", name);
                                try {
                                    if (diff.getScaleDown()) {
                                        log.info("Scaling Kafka Connect down to {} replicas", replicas);
                                        k8s.getDeploymentResource(namespace, name).scale(replicas, true);
                                    }

                                    log.info("Patching deployment");

                                    // TODO: If labels are changed, we should relabel also all replica sets and pods???\
                                    // TODO: Run separate diff for the service

                                    k8s.getDeploymentResource(namespace, name).replace(generateDeployment());
                                    k8s.getServiceResource(namespace, name).replace(generateService());


                                    if (diff.getScaleUp()) {
                                        log.info("Scaling Kafka Connect up to {} replicas", replicas);
                                        k8s.getDeploymentResource(namespace, name).scale(replicas, true);
                                    }

                                    // No need for rolling update - deployment will do it automatically?

                                    future.complete();
                                }
                                catch (Exception e) {
                                    log.error("Caught exceptoion: {}", e.toString());
                                    future.fail(e);
                                }
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
                .withLabels(getLabelsWithName())
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(getLabelsWithName())
                .withPorts(k8s.createServicePort(restApiPortName, restApiPort, restApiPort))
                .endSpec()
                .build();

        return svc;
    }

    private Deployment generateDeployment() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(new EnvVarBuilder().withName(KEY_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build(),
                        new EnvVarBuilder().withName(KEY_GROUP_ID).withValue(groupId).build(),
                        new EnvVarBuilder().withName(KEY_KEY_CONVERTER).withValue(keyConverter).build(),
                        new EnvVarBuilder().withName(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE).withValue(String.valueOf(keyConverterSchemasEnable)).build(),
                        new EnvVarBuilder().withName(KEY_VALUE_CONVERTER).withValue(valueConverter).build(),
                        new EnvVarBuilder().withName(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE).withValue(String.valueOf(valueConverterSchemasEnable)).build(),
                        new EnvVarBuilder().withName(KEY_CONFIG_STORAGE_REPLICATION_FACTOR).withValue(String.valueOf(configStorageReplicationFactor)).build(),
                        new EnvVarBuilder().withName(KEY_OFFSET_STORAGE_REPLICATION_FACTOR).withValue(String.valueOf(offsetStorageReplicationFactor)).build(),
                        new EnvVarBuilder().withName(KEY_STATUS_STORAGE_REPLICATION_FACTOR).withValue(String.valueOf(statusStorageReplicationFactor)).build())
                .withPorts(k8s.createContainerPort(restApiPortName, restApiPort))
                .withLivenessProbe(k8s.createHttpProbe(healthCheckPath, restApiPortName, healthCheckInitialDelay, healthCheckTimeout))
                .withReadinessProbe(k8s.createHttpProbe(healthCheckPath, restApiPortName, healthCheckInitialDelay, healthCheckTimeout))
                .build();

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .endMetadata()
                .withNewSpec()
                .withStrategy(new DeploymentStrategyBuilder().withType("RollingUpdate").withRollingUpdate(new RollingUpdateDeploymentBuilder().withMaxSurge(new IntOrString(1)).withMaxUnavailable(new IntOrString(0)).build()).build())
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
        return "kafka-connect::lock::" + name;
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

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setKeyConverter(String keyConverter) {
        this.keyConverter = keyConverter;
    }

    public void setKeyConverterSchemasEnable(Boolean keyConverterSchemasEnable) {
        this.keyConverterSchemasEnable = keyConverterSchemasEnable;
    }

    public void setValueConverter(String valueConverter) {
        this.valueConverter = valueConverter;
    }

    public void setValueConverterSchemasEnable(Boolean valueConverterSchemasEnable) {
        this.valueConverterSchemasEnable = valueConverterSchemasEnable;
    }

    public void setConfigStorageReplicationFactor(int configStorageReplicationFactor) {
        this.configStorageReplicationFactor = configStorageReplicationFactor;
    }

    public void setOffsetStorageReplicationFactor(int offsetStorageReplicationFactor) {
        this.offsetStorageReplicationFactor = offsetStorageReplicationFactor;
    }

    public void setStatusStorageReplicationFactor(int statusStorageReplicationFactor) {
        this.statusStorageReplicationFactor = statusStorageReplicationFactor;
    }

    public void setHealthCheckTimeout(int timeout) {
        this.healthCheckTimeout = timeout;
    }

    public void setHealthCheckInitialDelay(int delay) {
        this.healthCheckInitialDelay = delay;
    }
}
