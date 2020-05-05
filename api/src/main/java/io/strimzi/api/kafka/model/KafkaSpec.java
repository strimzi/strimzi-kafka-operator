/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code spec} of a {@link Kafka}.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "kafka", "zookeeper", "topicOperator",
                    "entityOperator", "clusterCa", "clientsCa",
                    "maintenance"})
@EqualsAndHashCode
public class KafkaSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private KafkaClusterSpec kafka;
    private ZookeeperClusterSpec zookeeper;
    private TopicOperatorSpec topicOperator;
    private EntityOperatorSpec entityOperator;
    private CertificateAuthority clusterCa;
    private JmxTransSpec jmxTrans;
    private KafkaExporterSpec kafkaExporter;
    private CruiseControlSpec cruiseControl;

    private CertificateAuthority clientsCa;
    private List<String> maintenanceTimeWindows;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Configuration of the Kafka cluster")
    @JsonProperty(required = true)
    public KafkaClusterSpec getKafka() {
        return kafka;
    }

    public void setKafka(KafkaClusterSpec kafka) {
        this.kafka = kafka;
    }

    @Description("Configuration of the ZooKeeper cluster")
    @JsonProperty(required = true)
    public ZookeeperClusterSpec getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(ZookeeperClusterSpec zookeeper) {
        this.zookeeper = zookeeper;
    }

    @Deprecated
    @DeprecatedProperty(
            movedToPath = "spec.entityOperator.topicOperator"
    )
    @Description("Configuration of the Topic Operator")
    public TopicOperatorSpec getTopicOperator() {
        return topicOperator;
    }

    @Deprecated
    public void setTopicOperator(TopicOperatorSpec topicOperator) {
        this.topicOperator = topicOperator;
    }

    @Description("Configuration of the Entity Operator")
    public EntityOperatorSpec getEntityOperator() {
        return entityOperator;
    }

    public void setEntityOperator(EntityOperatorSpec entityOperator) {
        this.entityOperator = entityOperator;
    }

    @Description("Configuration of the cluster certificate authority")
    public CertificateAuthority getClusterCa() {
        return clusterCa;
    }

    public void setClusterCa(CertificateAuthority clusterCa) {
        this.clusterCa = clusterCa;
    }

    @Description("Configuration of the clients certificate authority")
    public CertificateAuthority getClientsCa() {
        return clientsCa;
    }

    public void setClientsCa(CertificateAuthority clientsCa) {
        this.clientsCa = clientsCa;
    }

    @Description("A list of time windows for maintenance tasks (that is, certificates renewal). Each time window is defined by a cron expression.")
    public List<String> getMaintenanceTimeWindows() {
        return maintenanceTimeWindows;
    }

    @Description("Configuration for JmxTrans. When the property is present a JmxTrans deployment is created for gathering JMX metrics from each Kafka broker. " +
            "For more information see https://github.com/jmxtrans/jmxtrans[JmxTrans GitHub]")
    public JmxTransSpec getJmxTrans() {
        return jmxTrans;
    }

    public void setJmxTrans(JmxTransSpec jmxTrans) {
        this.jmxTrans = jmxTrans;
    }

    public void setMaintenanceTimeWindows(List<String> maintenanceTimeWindows) {
        this.maintenanceTimeWindows = maintenanceTimeWindows;
    }

    @Description("Configuration of the Kafka Exporter. Kafka Exporter can provide additional metrics, for example lag of consumer group at topic/partition.")
    public KafkaExporterSpec getKafkaExporter() {
        return kafkaExporter;
    }

    public void setKafkaExporter(KafkaExporterSpec kafkaExporter) {
        this.kafkaExporter = kafkaExporter;
    }

    @Description("Configuration for Cruise Control deployment. Deploys a Cruise Control instance when specified")
    public CruiseControlSpec getCruiseControl() {
        return cruiseControl;
    }

    public void setCruiseControl(CruiseControlSpec cruiseControl) {
        this.cruiseControl = cruiseControl;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        YAMLMapper mapper = new YAMLMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
