/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.components;

import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class used to create the JmxTrans queries and which remote hosts to output the results to
 * obj: references what Kafka MBean to reference
 * attr: an array of what MBean properties to target
 * outputDefinitionTemplates: Which hosts and how to output to the hosts
 */
public class JmxTransQueries implements Serializable {
    private static final long serialVersionUID = 1L;

    private String obj;

    private List<String> attr;

    private List<JmxTransOutputWriter> outputWriters;

    public String getObj() {
        return obj;
    }

    public void setObj(String obj) {
        this.obj = obj;
    }

    public List<String> getAttr() {
        return attr;
    }

    public void setAttr(List<String> attr) {
        this.attr = attr;
    }

    public List<JmxTransOutputWriter> getOutputWriters() {
        return outputWriters;
    }

    public void setOutputWriters(List<JmxTransOutputWriter> outputDefinitionTemplates) {
        this.outputWriters = outputDefinitionTemplates;
    }
}
