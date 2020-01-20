/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.components;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class used specify which remote host to output to and in what format to push it in
 * atClass: the format of the data to push to remote host
 * host: The host of the remote host to push to
 * port: The port of the remote host to push to
 * flushDelayInSeconds: how often to push the data in seconds
 */
public class JmxTransOutputWriter implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("@class")
    private String atClass;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private String host;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private int port;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private int flushDelayInSeconds;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private List<String> typeNames;

    public String getAtClass() {
        return atClass;
    }

    public void setAtClass(String atClass) {
        this.atClass = atClass;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getFlushDelayInSeconds() {
        return flushDelayInSeconds;
    }

    public void setFlushDelayInSeconds(int flushDelayInSeconds) {
        this.flushDelayInSeconds = flushDelayInSeconds;
    }

    public List<String> getTypeNames() {
        return typeNames;
    }

    public void setTypeNames(List<String> typeNames) {
        this.typeNames = typeNames;
    }
}
