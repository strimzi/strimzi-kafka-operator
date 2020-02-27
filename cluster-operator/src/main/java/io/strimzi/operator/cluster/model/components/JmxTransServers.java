/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.components;

import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class used to create the overall config file
 * Servers: A list of servers and what they will query and output
 */
public class JmxTransServers implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<JmxTransServer> servers;

    public List<JmxTransServer> getServers() {
        return servers;
    }

    public void setServers(List<JmxTransServer> servers) {
        this.servers = servers;
    }
}
