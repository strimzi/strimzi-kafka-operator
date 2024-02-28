/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.connect.build.Build;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "version", "replicas", "image", "bootstrapServers",
    "tls", "authentication", "config", "resources", "livenessProbe",
    "readinessProbe", "jvmOptions", "jmxOptions", "affinity", "tolerations",
    "logging", "clientRackInitImage", "rack", "metrics", "tracing",
    "template", "externalConfiguration", "storage"})
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
public class KafkaConnectSpec extends AbstractKafkaConnectSpec {

    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "ssl., sasl., security., listeners, plugin.path, rest., bootstrap.servers, consumer.interceptor.classes, producer.interceptor.classes";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm, ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols";

    private Map<String, Object> config = new HashMap<>(0);
    private String bootstrapServers;
    private ClientTls tls;
    private KafkaClientAuthentication authentication;
    private Build build;
    private SingleVolumeStorage storage;

    @Description("The Kafka Connect configuration. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("Bootstrap servers to connect to. This should be given as a comma separated list of _<hostname>_:_<port>_ pairs.")
    @JsonProperty(required = true)
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Description("TLS configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ClientTls getTls() {
        return tls;
    }

    public void setTls(ClientTls tls) {
        this.tls = tls;
    }

    @Description("Authentication configuration for Kafka Connect")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaClientAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaClientAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("Configures how the Connect container image should be built. " +
            "Optional.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Build getBuild() {
        return build;
    }

    public void setBuild(Build build) {
        this.build = build;
    }

    @Description("Storage configuration (disk). Cannot be updated.")
    @JsonProperty(required = true)
    public SingleVolumeStorage getStorage() {
        return storage;
    }

    public void setStorage(SingleVolumeStorage storage) {
        this.storage = storage;
    }
}
