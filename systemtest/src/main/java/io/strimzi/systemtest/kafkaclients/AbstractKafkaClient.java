/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalLoadBalancer;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.systemtest.kafkaclients.clientproperties.ConsumerProperties;
import io.strimzi.systemtest.kafkaclients.clientproperties.ProducerProperties;
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
    protected String listenerName;
    protected ProducerProperties producerProperties;
    protected ConsumerProperties consumerProperties;

    public static abstract class Builder<T extends Builder<T>> {

        private String topicName;
        protected Integer partition;
        private String namespaceName;
        private String clusterName;
        private int messageCount;
        private String consumerGroup;
        private String kafkaUsername;
        private SecurityProtocol securityProtocol;
        private String caCertName;
        protected String listenerName;
        private ProducerProperties producerProperties;
        private ConsumerProperties consumerProperties;

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

        public T withListenerName(String listenerName) {
            this.listenerName = listenerName;
            return self();
        }

        public T withProducerProperties(ProducerProperties producerProperties) {
            this.producerProperties = producerProperties;
            return self();
        }

        public T withConsumerProperties(ConsumerProperties consumerProperties) {
            this.consumerProperties =  consumerProperties;
            return self();
        }

        protected abstract AbstractKafkaClient build();

        // Subclasses must override this method to return "this" protected abstract T self();
        // for not explicit casting..
        protected abstract T self();
    }

    protected Builder<?> toBuilder(Builder<?> builder) {
        verifyEssentialAttributes(builder);

        return builder
            .withTopicName(builder.topicName)
            .withPartition(builder.partition)
            .withNamespaceName(builder.namespaceName)
            .withClusterName(builder.clusterName)
            .withMessageCount(builder.messageCount)
            .withConsumerGroupName(builder.consumerGroup)
            .withKafkaUsername(builder.kafkaUsername)
            .withSecurityProtocol(builder.securityProtocol)
            .withCertificateAuthorityCertificateName(builder.caCertName)
            .withListenerName(builder.listenerName)
            .withProducerProperties(builder.producerProperties)
            .withConsumerProperties(builder.consumerProperties);
    }

    protected AbstractKafkaClient(Builder<?> builder) {
        verifyEssentialAttributes(builder);

        topicName = builder.topicName;
        partition = builder.partition;
        namespaceName = builder.namespaceName;
        clusterName = builder.clusterName;
        messageCount = builder.messageCount;
        consumerGroup = builder.consumerGroup;
        kafkaUsername = builder.kafkaUsername;
        securityProtocol = builder.securityProtocol;
        caCertName = builder.caCertName;
        listenerName = builder.listenerName;
        producerProperties = builder.producerProperties;
        consumerProperties = builder.consumerProperties;
    }

    private void verifyEssentialAttributes(Builder<?> builder) {
        if (builder.topicName == null || builder.topicName.isEmpty()) throw new InvalidParameterException("Topic name is not set.");
        if (builder.namespaceName == null || builder.namespaceName.isEmpty()) throw new InvalidParameterException("Namespace name is not set.");
        if (builder.clusterName == null  || builder.clusterName.isEmpty()) throw  new InvalidParameterException("Cluster name is not set.");
        if (builder.messageCount <= 0) throw  new InvalidParameterException("Message count is less than 1");
        if (builder.consumerGroup == null || builder.consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            builder.consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }
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
     * @param listenerName name of the listener
     * @return bootstrap url as string
     */
    @SuppressWarnings("Regexp") // because of extBootstrapService.getSpec().getType().toLowerCase()
    @SuppressFBWarnings("DM_CONVERT_CASE")
    public static String getExternalBootstrapConnect(String namespace, String clusterName, String listenerName) {

        if (kubeClient(namespace).getClient().isAdaptable(OpenShiftClient.class)) {
            Route route = listenerName != null ?
                kubeClient(namespace).getClient().adapt(OpenShiftClient.class).routes().inNamespace(namespace).withName(clusterName + "-kafka-" + listenerName + "-bootstrap").get() :
                kubeClient(namespace).getClient().adapt(OpenShiftClient.class).routes().inNamespace(namespace).withName(clusterName + "-kafka-bootstrap").get();

            if (route != null && !route.getStatus().getIngress().isEmpty()) {
                return route.getStatus().getIngress().get(0).getHost() + ":443";
            }
        }

        NonNamespaceOperation<Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>> services = kubeClient(namespace).getClient().services().inNamespace(namespace);

        Service extBootstrapService = listenerName != null ?
            services.withName(KafkaResources.kafkaStatefulSetName(clusterName) + "-" + listenerName + "-bootstrap").get() :
            services.withName(externalBootstrapServiceName(clusterName)).get();

        if (extBootstrapService == null) {
            throw new RuntimeException("Kafka cluster " + clusterName + " doesn't have an external bootstrap service");
        }

        LOGGER.info("Using {}, is equal to {}", extBootstrapService.getSpec().getType(),  KafkaListenerExternalNodePort.TYPE_NODEPORT);

        switch (extBootstrapService.getSpec().getType().toLowerCase()) {
            case KafkaListenerExternalNodePort.TYPE_NODEPORT:
                return kubeClient().getNodeAddress() + ":" + extBootstrapService.getSpec().getPorts().get(0).getNodePort();
            case KafkaListenerExternalLoadBalancer.TYPE_LOADBALANCER:
                LoadBalancerIngress loadBalancerIngress = extBootstrapService.getStatus().getLoadBalancer().getIngress().get(0);
                String result = loadBalancerIngress.getHostname();

                if (result == null) {
                    result = loadBalancerIngress.getIp();
                }
                return result + ":" + extBootstrapService.getSpec().getPorts().get(0).getPort();
            default:
                throw new RuntimeException("Unexpected external bootstrap service" + extBootstrapService.getSpec().getType() + " for Kafka cluster " + clusterName);
        }
    }

    /**
     * Get external bootstrap connection
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @return bootstrap url as string
     */
    public static String getExternalBootstrapConnect(String namespace, String clusterName) {
        return getExternalBootstrapConnect(namespace, clusterName, null);
    }

    public ProducerProperties getProducerProperties() {
        return producerProperties;
    }
    public ConsumerProperties getConsumerProperties() {
        return consumerProperties;
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

    public String getTopicName() {
        return topicName;
    }
    public Integer getPartition() {
        return partition;
    }
    public String getNamespaceName() {
        return namespaceName;
    }
    public String getClusterName() {
        return clusterName;
    }
    public int getMessageCount() {
        return messageCount;
    }
    public String getConsumerGroup() {
        return consumerGroup;
    }
    public String getKafkaUsername() {
        return kafkaUsername;
    }
    public SecurityProtocol getSecurityProtocol() {
        return securityProtocol;
    }
    public String getCaCertName() {
        return caCertName;
    }
    public String getListenerName() {
        return listenerName;
    }

    @Override
    public String toString() {
        return "AbstractKafkaClient{" +
            "topicName='" + topicName + '\'' +
            ", partition=" + partition +
            ", namespaceName='" + namespaceName + '\'' +
            ", clusterName='" + clusterName + '\'' +
            ", messageCount=" + messageCount +
            ", consumerGroup='" + consumerGroup + '\'' +
            ", kafkaUsername='" + kafkaUsername + '\'' +
            ", securityProtocol=" + securityProtocol +
            ", caCertName='" + caCertName + '\'' +
            ", listenerName='" + listenerName + '\'' +
            ", producerProperties=" + producerProperties +
            ", consumerProperties=" + consumerProperties +
            '}';
    }
}
