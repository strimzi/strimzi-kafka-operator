/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures Ingress listeners
 */
@DescriptionFile 
@JsonPropertyOrder({"bootstrap", "brokers", "brokerCertAndKey"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class IngressListenerConfiguration extends KafkaListenerExternalConfiguration {
    private static final long serialVersionUID = 1L;

    private IngressListenerBootstrapConfiguration bootstrap;
    private List<IngressListenerBrokerConfiguration> brokers;

    @Description("External bootstrap ingress configuration")
    public IngressListenerBootstrapConfiguration getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(IngressListenerBootstrapConfiguration bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Description("External broker ingress configuration")
    public List<IngressListenerBrokerConfiguration> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<IngressListenerBrokerConfiguration> brokers) {
        this.brokers = brokers;
    }
}
