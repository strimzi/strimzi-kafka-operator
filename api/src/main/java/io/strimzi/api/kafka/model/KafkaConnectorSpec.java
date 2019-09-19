/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URL;
import java.util.List;

public class KafkaConnectorSpec {
    private ConnectCluster connectCluster;
    private String className;
    private Integer tasksMax;
    private List<String> topics;
    private List config;

    public List getConfig() {
        return config;
    }

    public List<String> getTopics() {
        return topics;
    }

    public Integer getTasksMax() {
        return tasksMax;
    }

    @JsonProperty("class")
    public String getClassName() {
        return className;
    }

    public ConnectCluster getConnectCluster() {
        return connectCluster;
    }

    public class ConnectCluster {
        private URL url;

        public URL getUrl() {
            return url;
        }
    }
}
