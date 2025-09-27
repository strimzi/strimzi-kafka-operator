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
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "version", "replicas", "image", "bootstrapServers", "groupId", "configStorageTopic",
    "statusStorageTopic", "offsetStorageTopic", "tls", "authentication", "config", "resources",
    "livenessProbe", "readinessProbe", "jvmOptions", "jmxOptions", "logging", "clientRackInitImage", "rack",
    "metricsConfig", "tracing", "template", "externalConfiguration", "build", "plugins" })
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true)
public class KafkaConnectSpec extends AbstractKafkaConnectSpec {
    public static final String FORBIDDEN_PREFIXES = "ssl., sasl., security., listeners, plugin.path, rest., bootstrap.servers, consumer.interceptor.classes, producer.interceptor.classes, prometheus.metrics.reporter.";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm, ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols";

    private Map<String, Object> config = new HashMap<>(0);
    private String bootstrapServers;
    private String groupId;
    private String configStorageTopic;
    private String statusStorageTopic;
    private String offsetStorageTopic;
    private ClientTls tls;
    private KafkaClientAuthentication authentication;
    private Build build;
    private List<MountedPlugin> plugins;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
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

    @Description("A unique string that identifies the Connect cluster group this worker belongs to.")
    @RequiredInVersions("v1+")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Description("The name of the Kafka topic where connector configurations are stored.")
    @RequiredInVersions("v1+")
    public String getConfigStorageTopic() {
        return configStorageTopic;
    }

    public void setConfigStorageTopic(String configStorageTopic) {
        this.configStorageTopic = configStorageTopic;
    }

    @Description("The name of the Kafka topic where connector and task status are stored")
    @RequiredInVersions("v1+")
    public String getStatusStorageTopic() {
        return statusStorageTopic;
    }

    public void setStatusStorageTopic(String statusStorageTopic) {
        this.statusStorageTopic = statusStorageTopic;
    }

    @Description("The name of the Kafka topic where source connector offsets are stored.")
    @RequiredInVersions("v1+")
    public String getOffsetStorageTopic() {
        return offsetStorageTopic;
    }

    public void setOffsetStorageTopic(String offsetStorageTopic) {
        this.offsetStorageTopic = offsetStorageTopic;
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

    @Description("List of connector plugins to mount into the `KafkaConnect` pod.")
    public List<MountedPlugin> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<MountedPlugin> plugins) {
        this.plugins = plugins;
    }
}
