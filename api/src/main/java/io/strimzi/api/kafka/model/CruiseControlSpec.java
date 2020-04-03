/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.api.kafka.model.template.CruiseControlTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "image", "config",
        "livenessProbe", "readinessProbe",
        "jvmOptions", "resources",
        "logging", "tlsSidecar", "template", "brokerCapacity"})
@EqualsAndHashCode
public class CruiseControlSpec implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    // For the full configuration list refer to https://github.com/linkedin/cruise-control/wiki/Configurations
    public static final String FORBIDDEN_PREFIXES = "bootstrap.servers, zookeeper., ssl., security., failed.brokers.zk.path,"
        + "webserver.http.port, webserver.http.address, webserver.api.urlprefix, metric.reporter.sampler.bootstrap.servers, metric.reporter.topic, metric.reporter.topic.pattern,"
        + "partition.metric.sample.store.topic, broker.metric.sample.store.topic, brokerCapacity.config.file, skip.sample.store.topic.rack.awareness.check, cruise.control.metrics.topic"
        + "cruise.control.metrics.reporter.bootstrap.servers, sasl. interceptor.classes";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "";

    private int replicas;
    private String image;
    private TlsSidecar tlsSidecar;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private JvmOptions jvmOptions;
    private Logging logging;
    private CruiseControlTemplate template;
    private BrokerCapacity brokerCapacity;
    private Map<String, Object> config = new HashMap<>(0);
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The number of pods in the `Deployment`.")
    @Minimum(0)
    @Maximum(1)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The docker image for the pods.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TlsSidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    @Description("The Cruise Control brokerCapacity configuration.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public BrokerCapacity getBrokerCapacity() {
        return brokerCapacity;
    }

    public void setBrokerCapacity(BrokerCapacity brokerCapacity) {
        this.brokerCapacity = brokerCapacity;
    }

    @Description("The Cruise Control configuration. For a full list of configuration options refer to" +
            " https://github.com/linkedin/cruise-control/wiki/Configurations Note that properties " +
            "with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("Logging configuration for Cruise Control.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JVM Options for the Cruise Control container")
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("CPU and memory resources to reserve for the Cruise Control container")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod liveness checking for the Cruise Control container")
    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Pod readiness checking for the Cruise Control container.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @Description("Template to specify how Cruise Control resources, `Deployments` and `Pods`, are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public CruiseControlTemplate getTemplate() {
        return template;
    }

    public void setTemplate(CruiseControlTemplate template) {
        this.template = template;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
