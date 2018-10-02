/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;

/**
 * Representation of a Strimzi-managed Topic Operator deployment.
 */
@Deprecated
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"watchedNamespace", "image",
        "reconciliationIntervalSeconds", "zookeeperSessionTimeoutSeconds",
        "affinity", "resources", "topicMetadataMaxAttempts", "tlsSidecar", "logging"})
public class TopicOperatorSpec extends EntityTopicOperatorSpec {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_TLS_SIDECAR_IMAGE = EntityOperatorSpec.DEFAULT_TLS_SIDECAR_IMAGE;

    private Affinity affinity;
    private TlsSidecar tlsSidecar;

    @Description("Pod affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TlsSidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }
}
