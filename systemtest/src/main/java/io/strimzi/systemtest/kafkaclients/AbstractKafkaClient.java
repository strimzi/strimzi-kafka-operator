/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public abstract class AbstractKafkaClient {

    private static final Logger LOGGER = LogManager.getLogger(AbstractKafkaClient.class);

    protected String topicName;
    protected Integer partition;
    protected String namespaceName;
    protected String clusterName;
    protected int messageCount;
    protected String consumerGroup;
    protected String kafkaUsername;
    protected SecurityProtocol securityProtocol;
    protected String caCertName;
    protected KafkaClientProperties clientProperties;

    public abstract static class Builder<T extends Builder<T>> {

        private String topicName;
        protected Integer partition;
        private String namespaceName;
        private String clusterName;
        private int messageCount;
        private String consumerGroup;
        private String kafkaUsername;
        private SecurityProtocol securityProtocol;
        private String caCertName;
        private KafkaClientProperties clientProperties;

        public T withTopicName(String topicName) {
            this.topicName = topicName;
            return self();
        }

        public T withPartition(Integer partition) {
            this.partition = partition;
            return self();
        }

        public T withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return self();
        }

        public T withClusterName(String clusterName) {
            this.clusterName = clusterName;
            return self();
        }

        public T withMessageCount(int messageCount) {
            this.messageCount = messageCount;
            return self();
        }

        public T withConsumerGroupName(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return self();
        }

        public T withKafkaUsername(String kafkaUsername) {
            this.kafkaUsername = kafkaUsername;
            return self();
        }

        public T withSecurityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return self();
        }

        public T withCertificateAuthorityCertificateName(String caCertName) {
            this.caCertName = caCertName;
            return self();
        }

        public T withKafkaClientProperties(KafkaClientProperties clientProperties) {
            this.clientProperties = clientProperties;
            return self();
        }

        protected abstract AbstractKafkaClient build();

        // Subclasses must override this method to return "this" protected abstract T self();
        // for not explicit casting..
        protected abstract T self();
    }

    protected AbstractKafkaClient(Builder<?> builder) {

        if (builder.topicName == null || builder.topicName.isEmpty()) throw new InvalidParameterException("Topic name is not set.");
        if (builder.namespaceName == null || builder.namespaceName.isEmpty()) throw new InvalidParameterException("Namespace name is not set.");
        if (builder.clusterName == null  || builder.clusterName.isEmpty()) throw  new InvalidParameterException("Cluster name is not set.");
        if (builder.messageCount <= 0) throw  new InvalidParameterException("Message count is less than 1");
        if (builder.consumerGroup == null || builder.consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            builder.consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }

        topicName = builder.topicName;
        partition = builder.partition;
        namespaceName = builder.namespaceName;
        clusterName = builder.clusterName;
        messageCount = builder.messageCount;
        consumerGroup = builder.consumerGroup;
        kafkaUsername = builder.kafkaUsername;
        securityProtocol = builder.securityProtocol;
        caCertName = builder.caCertName;
        clientProperties = builder.clientProperties;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public void verifyProducedAndConsumedMessages(int producedMessages, int consumedMessages) {
        if (producedMessages != consumedMessages) {
            LOGGER.info("Producer produced {} messages", producedMessages);
            LOGGER.info("Consumer consumed {} messages", consumedMessages);
            throw new RuntimeException("Producer or consumer does not produce or consume required message");
        }
    }

    /**
     * Get external bootstrap connection
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @return bootstrap url as string
     */
    public static String getExternalBootstrapConnect(String namespace, String clusterName) {
        if (kubeClient(namespace).getClient().isAdaptable(OpenShiftClient.class)) {
            Route route = kubeClient(namespace).getClient().adapt(OpenShiftClient.class).routes().inNamespace(namespace).withName(clusterName + "-kafka-bootstrap").get();
            if (route != null && !route.getStatus().getIngress().isEmpty()) {
                return route.getStatus().getIngress().get(0).getHost() + ":443";
            }
        }

        Service extBootstrapService = kubeClient(namespace).getClient().services()
                .inNamespace(namespace)
                .withName(externalBootstrapServiceName(clusterName))
                .get();

        if (extBootstrapService == null) {
            throw new RuntimeException("Kafka cluster " + clusterName + " doesn't have an external bootstrap service");
        }

        String extBootstrapServiceType = extBootstrapService.getSpec().getType();

        if (extBootstrapServiceType.equals("NodePort")) {
            int port = extBootstrapService.getSpec().getPorts().get(0).getNodePort();
            String externalAddress = kubeClient(namespace).listNodes().get(0).getStatus().getAddresses().get(0).getAddress();
            return externalAddress + ":" + port;
        } else if (extBootstrapServiceType.equals("LoadBalancer")) {
            LoadBalancerIngress loadBalancerIngress = extBootstrapService.getStatus().getLoadBalancer().getIngress().get(0);
            String result = loadBalancerIngress.getHostname();

            if (result == null) {
                result = loadBalancerIngress.getIp();
            }
            return result + ":9094";
        } else {
            throw new RuntimeException("Unexpected external bootstrap service" + extBootstrapServiceType + " for Kafka cluster " + clusterName);
        }
    }

    public KafkaClientProperties getClientProperties() {
        return clientProperties;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setKafkaUsername(String kafkaUsername) {
        this.kafkaUsername = kafkaUsername;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setCaCertName(String caCertName) {
        this.caCertName = caCertName;
    }
    @Override
    public String toString() {
        return "AbstractKafkaClient{" +
            "topicName='" + topicName + '\'' +
            ", namespaceName='" + namespaceName + '\'' +
            ", clusterName='" + clusterName + '\'' +
            ", messageCount=" + messageCount +
            ", consumerGroup='" + consumerGroup + '\'' +
            ", kafkaUsername='" + kafkaUsername + '\'' +
            ", securityProtocol='" + securityProtocol + '\'' +
            ", caCertName='" + caCertName + '\'' +
            ", clientProperties=" + clientProperties +
            '}';
    }
}
