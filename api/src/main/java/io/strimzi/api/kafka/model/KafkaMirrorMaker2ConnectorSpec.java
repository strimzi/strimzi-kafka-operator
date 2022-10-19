/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"tasksMax", "autoRestartConnectorsAndTasks", "config"})
@EqualsAndHashCode(callSuper = true)
public class KafkaMirrorMaker2ConnectorSpec extends AbstractConnectorSpec {
    private static final long serialVersionUID = 1L;

    private AutoRestart autoRestartConnectorsAndTasks = new AutoRestart();

    @Description("Automatic restart of connectors and tasks configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public AutoRestart getAutoRestartConnectorsAndTasks() {
        return autoRestartConnectorsAndTasks;
    }

    public void setAutoRestartConnectorsAndTasks(AutoRestart autoRestart) {
        this.autoRestartConnectorsAndTasks = autoRestart;
    }
}
