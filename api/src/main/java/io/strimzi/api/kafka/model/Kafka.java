/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Strimzi-managed Kafka "cluster".
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonPropertyOrder({ "replicas", "image", "storage", "rackConfig", "brokerRackInitImage",
        "livenessProbe", "readinessProbe", "jvmOptions", "affinity", "metrics", "tlsSidecar"})
public class Kafka extends ReplicatedJvmPods {

    public static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_IMAGE", "strimzi/kafka:latest");
    public static final String DEFAULT_INIT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_INIT_IMAGE", "strimzi/kafka-init:latest");
    public static final String DEFAULT_TLS_SIDECAR_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE", "strimzi/kafka-stunnel:latest");

    /*
    // Forbidden prefixes were temporarily modified to allow configuration of Authentication and Authorization before we
    // have UserOperator implemented.

    public static final String FORBIDDEN_PREFIXES = "listeners, advertised., broker., listener., host.name, port, "
            + "inter.broker.listener.name, sasl., ssl., security., password., principal.builder.class, log.dir, "
            + "zookeeper.connect, zookeeper.set.acl, authorizer., super.user";
    */

    public static final String FORBIDDEN_PREFIXES = "listeners, advertised., broker., listener.name.replication., "
            + "listener.name.clienttls.ssl.truststore, listener.name.clienttls.ssl.keystore, host.name, port, "
            + "inter.broker.listener.name, sasl., ssl., security., password., principal.builder.class, log.dir, "
            + "zookeeper.connect, zookeeper.set.acl, super.user";

    protected Storage storage;

    private Map<String, Object> config = new HashMap<>(0);

    private String brokerRackInitImage;

    private Rack rack;

    private Logging logging;

    private Sidecar tlsSidecar;

    @Description("The kafka broker config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("The image of the init container used for initializing the `broker.rack`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getBrokerRackInitImage() {
        return brokerRackInitImage;
    }

    public void setBrokerRackInitImage(String brokerRackInitImage) {
        this.brokerRackInitImage = brokerRackInitImage;
    }

    @Description("Configuration of the `broker.rack` broker config.")
    @JsonProperty("rack")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Rack getRack() {
        return rack;
    }

    public void setRack(Rack rack) {
        this.rack = rack;
    }

    @Description("Storage configuration (disk). Cannot be updated.")
    @JsonProperty(required = true)
    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Description("Logging configuration for Kafka")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Sidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(Sidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }
}
