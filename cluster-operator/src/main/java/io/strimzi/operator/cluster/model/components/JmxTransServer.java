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
 * Wrapper class used to create the per broker JmxTrans queries and which remote hosts it will output to
 * host: references which broker the JmxTrans will read from
 * port: port of the host
 * queries: what queries are passed in
 */
public class JmxTransServer implements Serializable {
    private static final long serialVersionUID = 1L;

    private String host;

    private int port;

    @JsonProperty("queries")
    private List<JmxTransQueries> queriesTemplate;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private String username;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String password;

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

    public List<JmxTransQueries> getQueries() {
        return queriesTemplate;
    }

    public void setQueries(List<JmxTransQueries> queriesTemplate) {
        this.queriesTemplate = queriesTemplate;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
