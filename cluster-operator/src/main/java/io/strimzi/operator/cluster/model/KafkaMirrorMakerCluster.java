/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.extensions.RollingUpdateDeploymentBuilder;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.KafkaMirrorMakerAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaMirrorMakerClientSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerSpec;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaMirrorMakerCluster extends AbstractModel {

    private static final String NAME_SUFFIX = "-mirror-maker";

    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    protected static final String TLS_CERTS_VOLUME_MOUNT_CONSUMER = "/opt/kafka/consumer-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT_CONSUMER = "/opt/kafka/consumer-password/";
    protected static final String TLS_CERTS_VOLUME_MOUNT_PRODUCER = "/opt/kafka/producer-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT_PRODUCER = "/opt/kafka/producer-password/";

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 3;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 60;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final boolean DEFAULT_KAFKA_MIRRORMAKER_METRICS_ENABLED = false;

    // Kafka Mirror Maker configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_METRICS_ENABLED = "KAFKA_MIRRORMAKER_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER = "KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER = "KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER = "KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER = "KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER = "KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER = "KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_GROUPID_CONSUMER = "KAFKA_MIRRORMAKER_GROUPID_CONSUMER";

    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER = "KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER = "KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER = "KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER = "KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER = "KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER = "KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER";

    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_WHITELIST = "KAFKA_MIRRORMAKER_WHITELIST";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_NUMSTREAMS = "KAFKA_MIRRORMAKER_NUMSTREAMS";

    protected String whitelist;

    protected KafkaMirrorMakerClientSpec producer;
    protected CertAndKeySecretSource producerTlsAuthCertAndKey;
    private String producerUsername;
    private PasswordSecretSource producerPasswordSecret;
    protected KafkaMirrorMakerConsumerSpec consumer;
    protected CertAndKeySecretSource consumerTlsAuthCertAndKey;
    private String consumerUsername;
    private PasswordSecretSource consumerPasswordSecret;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Mirror Maker cluster resources are going to be created
     * @param cluster   overall cluster name
     * @param labels    labels to add to the cluster
     */
    protected KafkaMirrorMakerCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaMirrorMakerClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.validLoggerFields = getDefaultLogConfig();
        this.ancillaryConfigName = logAndMetricsConfigName(cluster);
        this.image = KafkaMirrorMakerSpec.DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/";
        this.livenessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_MIRRORMAKER_METRICS_ENABLED;

        this.mountPath = "/var/lib/kafka";
        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";
    }

    public static String kafkaMirrorMakerClusterName(String cluster) {
        return cluster + KafkaMirrorMakerCluster.NAME_SUFFIX;
    }

    public static String serviceName(String cluster) {
        return kafkaMirrorMakerClusterName(cluster);
    }

    public static String logAndMetricsConfigName(String cluster) {
        return cluster + KafkaMirrorMakerCluster.METRICS_AND_LOG_CONFIG_SUFFIX;
    }


    private static void setClientAuth(KafkaMirrorMakerCluster kafkaMirrorMakerCluster, KafkaMirrorMakerClientSpec client) {
        if (client != null && client.getAuthentication() != null) {
            if (client.getAuthentication() instanceof KafkaMirrorMakerAuthenticationTls) {
                KafkaMirrorMakerAuthenticationTls clientAuth = (KafkaMirrorMakerAuthenticationTls) client.getAuthentication();
                CertAndKeySecretSource clientCertificateAndKey = clientAuth.getCertificateAndKey();
                if (clientCertificateAndKey != null) {
                    if (client.getTls() == null) {
                        log.warn("TLS configuration missing: related TLS client authentication will not work properly");
                    }
                    if (client instanceof KafkaMirrorMakerConsumerSpec)
                        kafkaMirrorMakerCluster.setConsumerTlsAuthCertAndKey(clientCertificateAndKey);
                    else if (client instanceof KafkaMirrorMakerProducerSpec)
                        kafkaMirrorMakerCluster.setProducerTlsAuthCertAndKey(clientCertificateAndKey);
                } else {
                    log.warn("TLS Client authentication selected, but no certificate and key configured.");
                    throw new InvalidResourceException("TLS Client authentication selected, but no certificate and key configured.");
                }
            } else if (client.getAuthentication() instanceof KafkaMirrorMakerAuthenticationScramSha512) {
                KafkaMirrorMakerAuthenticationScramSha512 auth = (KafkaMirrorMakerAuthenticationScramSha512) client.getAuthentication();
                if (auth.getUsername() != null && auth.getPasswordSecret() != null) {
                    if (client instanceof KafkaMirrorMakerConsumerSpec)
                        kafkaMirrorMakerCluster.setConsumerUsernameAndPassword(auth.getUsername(), auth.getPasswordSecret());
                    else if (client instanceof KafkaMirrorMakerProducerSpec)
                        kafkaMirrorMakerCluster.setProducerUsernameAndPassword(auth.getUsername(), auth.getPasswordSecret());
                } else {
                    log.warn("SCRAM-SHA-512 authentication selected, but no username and password configured.");
                    throw new InvalidResourceException("SCRAM-SHA-512 authentication selected, but no username and password configured.");
                }
            }
        }
    }

    public static KafkaMirrorMakerCluster fromCrd(KafkaMirrorMaker kafkaMirrorMaker) {
        KafkaMirrorMakerCluster kafkaMirrorMakerCluster = new KafkaMirrorMakerCluster(kafkaMirrorMaker.getMetadata().getNamespace(),
                kafkaMirrorMaker.getMetadata().getName(),
                Labels.fromResource(kafkaMirrorMaker).withKind(kafkaMirrorMaker.getKind()));

        kafkaMirrorMakerCluster.setReplicas(kafkaMirrorMaker.getSpec() != null && kafkaMirrorMaker.getSpec().getReplicas() > 0 ? kafkaMirrorMaker.getSpec().getReplicas() : DEFAULT_REPLICAS);

        if (kafkaMirrorMaker.getSpec() != null) {
            kafkaMirrorMakerCluster.setWhitelist(kafkaMirrorMaker.getSpec().getWhitelist());
            kafkaMirrorMakerCluster.setProducer(kafkaMirrorMaker.getSpec().getProducer());
            kafkaMirrorMakerCluster.setConsumer(kafkaMirrorMaker.getSpec().getConsumer());
            String image = kafkaMirrorMaker.getSpec().getImage();
            kafkaMirrorMakerCluster.setImage(image == null ? KafkaMirrorMakerSpec.DEFAULT_IMAGE : image);
            kafkaMirrorMakerCluster.setLogging(kafkaMirrorMaker.getSpec().getLogging());

            Map<String, Object> metrics = kafkaMirrorMaker.getSpec().getMetrics();
            if (metrics != null && !metrics.isEmpty()) {
                kafkaMirrorMakerCluster.setMetricsEnabled(true);
                kafkaMirrorMakerCluster.setMetricsConfig(metrics.entrySet());
            }
        }

        setClientAuth(kafkaMirrorMakerCluster, kafkaMirrorMaker.getSpec().getConsumer());
        setClientAuth(kafkaMirrorMakerCluster, kafkaMirrorMaker.getSpec().getProducer());

        kafkaMirrorMakerCluster.setOwnerReference(kafkaMirrorMaker);
        return kafkaMirrorMakerCluster;
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(1);
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
            return createService("ClusterIP", ports, getPrometheusAnnotations());
        } else {
            return null;
        }
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>(1);
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));

        createClientSecretVolume(producer, producerTlsAuthCertAndKey, producerPasswordSecret, volumeList);
        createClientSecretVolume(consumer, consumerTlsAuthCertAndKey, consumerPasswordSecret, volumeList);

        return volumeList;
    }

    protected void createClientSecretVolume(KafkaMirrorMakerClientSpec client, CertAndKeySecretSource clientTlsAuthCertAndKey, PasswordSecretSource clientPasswordSecret, List<Volume> volumeList) {
        if (client.getTls() != null && client.getTls().getTrustedCertificates() != null && client.getTls().getTrustedCertificates().size() > 0) {
            for (CertSecretSource certSecretSource: client.getTls().getTrustedCertificates()) {
                // skipping if a volume with same Secret name was already added
                if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                    volumeList.add(createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName()));
                }
            }
        }

        if (clientTlsAuthCertAndKey != null) {
            if (!volumeList.stream().anyMatch(v -> v.getName().equals(clientTlsAuthCertAndKey.getSecretName()))) {
                volumeList.add(createSecretVolume(clientTlsAuthCertAndKey.getSecretName(), clientTlsAuthCertAndKey.getSecretName()));
            }
        } else if (clientPasswordSecret != null)  {
            volumeList.add(createSecretVolume(clientPasswordSecret.getSecretName(), clientPasswordSecret.getSecretName()));
        }
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(1);
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        /** producer auth*/
        if (producer.getTls() != null && producer.getTls().getTrustedCertificates() != null && producer.getTls().getTrustedCertificates().size() > 0) {
            for (CertSecretSource certSecretSource: producer.getTls().getTrustedCertificates()) {
                // skipping if a volume mount with same Secret name was already added
                if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                    volumeMountList.add(createVolumeMount(certSecretSource.getSecretName(),
                            TLS_CERTS_VOLUME_MOUNT_PRODUCER + certSecretSource.getSecretName()));
                }
            }
        }
        if (producerTlsAuthCertAndKey != null) {
            // skipping if a volume mount with same Secret name was already added
            if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(producerTlsAuthCertAndKey.getSecretName()))) {
                volumeMountList.add(createVolumeMount(producerTlsAuthCertAndKey.getSecretName(),
                        TLS_CERTS_VOLUME_MOUNT_PRODUCER + producerTlsAuthCertAndKey.getSecretName()));
            }
        } else if (producerPasswordSecret != null)  {
            volumeMountList.add(createVolumeMount(producerPasswordSecret.getSecretName(),
                    PASSWORD_VOLUME_MOUNT_PRODUCER + producerPasswordSecret.getSecretName()));
        }

        /** consumer auth*/
        if (consumer.getTls() != null && consumer.getTls().getTrustedCertificates() != null && consumer.getTls().getTrustedCertificates().size() > 0) {
            for (CertSecretSource certSecretSource: consumer.getTls().getTrustedCertificates()) {
                // skipping if a volume mount with same Secret name was already added
                if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                    volumeMountList.add(createVolumeMount(certSecretSource.getSecretName(),
                            TLS_CERTS_VOLUME_MOUNT_CONSUMER + certSecretSource.getSecretName()));
                }
            }
        }
        if (consumerTlsAuthCertAndKey != null) {
            // skipping if a volume mount with same Secret name was already added
            if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(consumerTlsAuthCertAndKey.getSecretName()))) {
                volumeMountList.add(createVolumeMount(consumerTlsAuthCertAndKey.getSecretName(),
                        TLS_CERTS_VOLUME_MOUNT_CONSUMER + consumerTlsAuthCertAndKey.getSecretName()));
            }
        } else if (consumerPasswordSecret != null)  {
            volumeMountList.add(createVolumeMount(consumerPasswordSecret.getSecretName(),
                    PASSWORD_VOLUME_MOUNT_CONSUMER + consumerPasswordSecret.getSecretName()));
        }

        return volumeMountList;
    }

    public Deployment generateDeployment(Map<String, String> annotations) {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                annotations,
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                getVolumes());
    }

    @Override
    protected List<Container> getContainers() {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withVolumeMounts(getVolumeMounts())
                .withResources(resources(getResources()))
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER, consumer.getBootstrapServers()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER, producer.getBootstrapServers()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_WHITELIST, whitelist));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_GROUPID_CONSUMER, consumer.getGroupId()));
        if (consumer.getNumStreams() != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_NUMSTREAMS, Integer.toString(consumer.getNumStreams())));
        }

        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);

        /** consumer */
        if (consumer.getTls() != null && consumer.getTls().getTrustedCertificates() != null && consumer.getTls().getTrustedCertificates().size() > 0) {
            StringBuilder sb = new StringBuilder();
            boolean separator = false;
            for (CertSecretSource certSecretSource: consumer.getTls().getTrustedCertificates()) {
                if (separator) {
                    sb.append(";");
                }
                sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                separator = true;
            }
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER, sb.toString()));
        }
        if (consumerTlsAuthCertAndKey != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER,
                    String.format("%s/%s", consumerTlsAuthCertAndKey.getSecretName(), consumerTlsAuthCertAndKey.getCertificate())));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER,
                    String.format("%s/%s", consumerTlsAuthCertAndKey.getSecretName(), consumerTlsAuthCertAndKey.getKey())));
        } else if (consumerPasswordSecret != null)  {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER, consumerUsername));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER,
                    String.format("%s/%s", consumerPasswordSecret.getSecretName(), consumerPasswordSecret.getPassword())));
        }

        /** producer */
        if (producer.getTls() != null && producer.getTls().getTrustedCertificates() != null && producer.getTls().getTrustedCertificates().size() > 0) {
            StringBuilder sb = new StringBuilder();
            boolean separator = false;
            for (CertSecretSource certSecretSource: producer.getTls().getTrustedCertificates()) {
                if (separator) {
                    sb.append(";");
                }
                sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                separator = true;
            }
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER, sb.toString()));
        }
        if (producerTlsAuthCertAndKey != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER,
                    String.format("%s/%s", producerTlsAuthCertAndKey.getSecretName(), producerTlsAuthCertAndKey.getCertificate())));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER,
                    String.format("%s/%s", producerTlsAuthCertAndKey.getSecretName(), producerTlsAuthCertAndKey.getKey())));
        } else if (producerPasswordSecret != null)  {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER, producerUsername));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER,
                    String.format("%s/%s", producerPasswordSecret.getSecretName(), producerPasswordSecret.getPassword())));
        }

        return varList;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "mirrorMakerDefaultLoggingProperties";
    }

    public void setWhitelist(String whitelist) {
        this.whitelist = whitelist;
    }

    public void setProducer(KafkaMirrorMakerClientSpec producer) {
        this.producer = producer;
    }

    public void setConsumer(KafkaMirrorMakerConsumerSpec consumer) {
        this.consumer = consumer;
    }

    public void setConsumerTlsAuthCertAndKey(CertAndKeySecretSource consumerTlsAuthCertAndKey) {
        this.consumerTlsAuthCertAndKey = consumerTlsAuthCertAndKey;
    }

    public void setProducerTlsAuthCertAndKey(CertAndKeySecretSource producerTlsAuthCertAndKey) {
        this.producerTlsAuthCertAndKey = producerTlsAuthCertAndKey;
    }

    private void setConsumerUsernameAndPassword(String username, PasswordSecretSource passwordSecret) {
        this.consumerUsername = username;
        this.consumerPasswordSecret = passwordSecret;
    }

    private void setProducerUsernameAndPassword(String username, PasswordSecretSource passwordSecret) {
        this.producerUsername = username;
        this.producerPasswordSecret = passwordSecret;
    }

    protected String getWhitelist() {
        return whitelist;
    }
}
