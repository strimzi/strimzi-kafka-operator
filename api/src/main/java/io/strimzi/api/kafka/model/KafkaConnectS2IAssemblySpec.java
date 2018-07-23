/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "replicas", "image",
        "livenessProbe", "readinessProbe", "jvmOptions", "affinity", "metrics"})
public class KafkaConnectS2IAssemblySpec extends KafkaConnectAssemblySpec {

    private static final long serialVersionUID = 1L;

    private boolean insecureSourceRepository = false;

    @Description("When true this configures the source repository with the 'Local' reference policy " +
            "and an import policy that accepts insecure source tags.")
    public boolean isInsecureSourceRepository() {
        return insecureSourceRepository;
    }

    public void setInsecureSourceRepository(boolean insecureSourceRepository) {
        this.insecureSourceRepository = insecureSourceRepository;
    }
}
