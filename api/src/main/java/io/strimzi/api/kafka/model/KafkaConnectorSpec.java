/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;

import java.io.Serializable;
import java.util.List;

@JsonPropertyOrder({"connectCluster", "class", "tasksMax", "topics", "config"})
public class KafkaConnectorSpec implements Serializable {
    private ConnectCluster connectCluster;
    private String className;
    private Integer tasksMax;
    private List<String> topics;
    private List<KafkaConnectorConfig> config;

    public ConnectCluster getConnectCluster() {
        return connectCluster;
    }

    @Description("The Class for the Kafka Connector")
    @JsonProperty("class")
    public String getClassName() {
        return className;
    }

    @Description("The maximum number of tasks for the Kafka Connector")
    @Minimum(1)
    public Integer getTasksMax() {
        return tasksMax;
    }

    @Description("A list of topics to use as input for the Kafka Connector")
    public List<String> getTopics() {
        return topics;
    }

    @Description("The Config for the Kafka Connector Spec.")
    public List<KafkaConnectorConfig> getConfig() {
        return config;
    }

    public class ConnectCluster {
        private String url;

        public String getUrl() {
            return url;
        }
    }
}
