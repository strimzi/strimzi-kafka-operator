/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Configures overrides for external bootstrap and broker services and advertised addresses
 */
@JsonPropertyOrder({"bootstrap", "brokers"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
public class KafkaExternalServiceOverrides implements Serializable {
    private static final long serialVersionUID = 8852179451894474397L;

    private KafkaExternalBootstrapService bootstrap;
    private List<KafkaExternalBrokerService> brokers;

    public KafkaExternalServiceOverrides() {
    }

    public KafkaExternalServiceOverrides(KafkaExternalBootstrapService bootstrap, List<KafkaExternalBrokerService> brokers) {
        this.bootstrap = bootstrap;
        this.brokers = brokers;
    }

    @Description("External bootstrap service configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaExternalBootstrapService getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(KafkaExternalBootstrapService bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Description("External broker services configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<KafkaExternalBrokerService> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<KafkaExternalBrokerService> brokers) {
        this.brokers = brokers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaExternalServiceOverrides that = (KafkaExternalServiceOverrides) o;
        return Objects.equals(bootstrap, that.bootstrap) &&
            Objects.equals(brokers, that.brokers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bootstrap, brokers);
    }

    @Override
    public String toString() {
        return "KafkaExternalServiceOverrides{" +
            "bootstrap=" + bootstrap +
            ", brokers=" + brokers +
            '}';
    }
}
