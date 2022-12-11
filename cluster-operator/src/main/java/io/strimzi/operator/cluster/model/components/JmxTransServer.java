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
 * Wrapper class used to create the per broker JmxTrans queries and which remote hosts it will output to
 * host: references which broker the JmxTrans will read from
 * port: port of the host
 * queries: what queries are passed in
 */
public class JmxTransServer implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Server host
     */
    private String host;

    /**
     * Server port
     */
    private int port;

    /**
     * List of JMXTrans queries
     */
    @JsonProperty("queries")
    private List<JmxTransQueries> queriesTemplate;

    /**
     * Server username
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private String username;

    /**
     * Server password
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private String password;

    /**
     * @return  Server host
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host field
     *
     * @param host  Server host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return  Server port
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port field
     *
     * @param port  Server port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return  List of JMXTrans queries
     */
    public List<JmxTransQueries> getQueries() {
        return queriesTemplate;
    }

    /**
     * Sets the list of JMXTrans queries
     *
     * @param queriesTemplate   List with queries
     */
    public void setQueries(List<JmxTransQueries> queriesTemplate) {
        this.queriesTemplate = queriesTemplate;
    }

    /**
     * @return  Server username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the server username
     *
     * @param username  Server username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return  Server password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the server password
     *
     * @param password  Server password
     */
    public void setPassword(String password) {
        this.password = password;
    }
}
