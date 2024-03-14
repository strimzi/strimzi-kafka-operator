/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.jmxtrans.JmxTransSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterSpec;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterSpec;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

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
    "maintenance", "cruiseControl", "jmxTrans", "kafkaExporter", "maintenanceTimeWindows"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaSpec extends Spec {
    private static final long serialVersionUID = 1L;

    private KafkaClusterSpec kafka;
    private ZookeeperClusterSpec zookeeper;
    private EntityOperatorSpec entityOperator;
    private CertificateAuthority clusterCa;
    @SuppressWarnings("deprecation")
    private JmxTransSpec jmxTrans;
    private KafkaExporterSpec kafkaExporter;
    private CruiseControlSpec cruiseControl;

    private CertificateAuthority clientsCa;
    private List<String> maintenanceTimeWindows;

    @Description("Configuration of the Kafka cluster")
    @JsonProperty(required = true)
    public KafkaClusterSpec getKafka() {
        return kafka;
    }

    public void setKafka(KafkaClusterSpec kafka) {
        this.kafka = kafka;
    }

    @Description("Configuration of the ZooKeeper cluster. This section is required when running a ZooKeeper-based Apache Kafka cluster.")
    public ZookeeperClusterSpec getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(ZookeeperClusterSpec zookeeper) {
        this.zookeeper = zookeeper;
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

    @Deprecated
    @DeprecatedProperty(description = "JMXTrans is deprecated and related resources removed in Strimzi 0.35.0.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Description("As of Strimzi 0.35.0, JMXTrans is not supported anymore and this option is ignored.")
    public JmxTransSpec getJmxTrans() {
        return jmxTrans;
    }

    @Deprecated
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
}
