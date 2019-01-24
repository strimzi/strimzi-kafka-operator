/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configures external bootstrap service
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
public class KafkaExternalBootstrapService implements Serializable {
    private static final long serialVersionUID = 7105212236247449919L;

    private Integer nodePort;

    public KafkaExternalBootstrapService() {
    }

    public KafkaExternalBootstrapService(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Description("Bootstrap service node port")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaExternalBootstrapService that = (KafkaExternalBootstrapService) o;
        return Objects.equals(nodePort, that.nodePort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodePort);
    }

    @Override
    public String toString() {
        return "KafkaExternalBootstrapService{" +
            "nodePort=" + nodePort +
            '}';
    }
}
