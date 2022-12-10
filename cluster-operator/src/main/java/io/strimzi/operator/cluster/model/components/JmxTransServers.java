/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.components;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class used to create the overall config file
 * Servers: A list of servers and what they will query and output
 */
public class JmxTransServers implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * List of JMX Trans servers
     */
    private List<JmxTransServer> servers;

    /**
     * @return  List of JMXTrans servers
     */
    public List<JmxTransServer> getServers() {
        return servers;
    }

    /**
     * Sets the JMXTrans servers
     *
     * @param servers   List ofJMX Trans servers
     */
    public void setServers(List<JmxTransServer> servers) {
        this.servers = servers;
    }
}
