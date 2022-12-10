/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.components;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
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
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Class for formatting the data output
     */
    @JsonProperty("@class")
    private String atClass;

    /**
     * Host where the data should be sent / written to
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private String host;

    /**
     * Port where the data should be sent / written to
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private int port;

    /**
     * How often will the data be flushed
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private int flushDelayInSeconds;

    /**
     * List with type names
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private List<String> typeNames;

    /**
     * @return  The class for formatting the data output
     */
    public String getAtClass() {
        return atClass;
    }

    /**
     * Sets the class for formatting the data output
     *
     * @param atClass   Output class
     */
    public void setAtClass(String atClass) {
        this.atClass = atClass;
    }

    /**
     * @return  The host where the data should be sent / written to
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host where the date should be sent / written to
     *
     * @param host  Hostname
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return  The port where the data should be sent / written to
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port where the date should be sent / written to
     *
     * @param port  Port number
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return  How often will the data be flushed
     */
    public int getFlushDelayInSeconds() {
        return flushDelayInSeconds;
    }

    /**
     * Sets the flush delay
     *
     * @param flushDelayInSeconds   Flush delay in seconds
     */
    public void setFlushDelayInSeconds(int flushDelayInSeconds) {
        this.flushDelayInSeconds = flushDelayInSeconds;
    }

    /**
     * @return  List with type names
     */
    public List<String> getTypeNames() {
        return typeNames;
    }

    /**
     * Sets the type names
     *
     * @param typeNames     List with type names
     */
    public void setTypeNames(List<String> typeNames) {
        this.typeNames = typeNames;
    }
}
