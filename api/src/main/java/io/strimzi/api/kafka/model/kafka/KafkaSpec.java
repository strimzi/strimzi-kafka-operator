/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterSpec;
import io.strimzi.api.kafka.model.kafka.sova.SovaSpec;
import io.strimzi.crdgenerator.annotations.DeprecatedProperty;
import io.strimzi.crdgenerator.annotations.Description;
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
@JsonPropertyOrder({ "kafka", "entityOperator", "clusterCa", "clientsCa", "cruiseControl", "kafkaExporter", "extraMetrics", "maintenanceTimeWindows"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaSpec extends Spec {
    private KafkaClusterSpec kafka;
    private EntityOperatorSpec entityOperator;
    private CertificateAuthority clusterCa;
    private KafkaExporterSpec kafkaExporter;
    private SovaSpec extraMetrics;
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

    public void setMaintenanceTimeWindows(List<String> maintenanceTimeWindows) {
        this.maintenanceTimeWindows = maintenanceTimeWindows;
    }

    @Description("Configuration of the Kafka Exporter. Kafka Exporter can provide additional metrics, for example lag of consumer group at topic/partition.")
    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.extraMetrics")
    public KafkaExporterSpec getKafkaExporter() {
        return kafkaExporter;
    }

    @Deprecated
    public void setKafkaExporter(KafkaExporterSpec kafkaExporter) {
        this.kafkaExporter = kafkaExporter;
    }

    @Description("Configuration of Sova. Sova can provide additional metrics, for example lag of consumer group at topic/partition.")
    public SovaSpec getExtraMetrics() {
        return extraMetrics;
    }

    public void setExtraMetrics(SovaSpec extraMetrics) {
        this.extraMetrics = extraMetrics;
    }

    @Description("Configuration for Cruise Control deployment. Deploys a Cruise Control instance when specified")
    public CruiseControlSpec getCruiseControl() {
        return cruiseControl;
    }

    public void setCruiseControl(CruiseControlSpec cruiseControl) {
        this.cruiseControl = cruiseControl;
    }
}
