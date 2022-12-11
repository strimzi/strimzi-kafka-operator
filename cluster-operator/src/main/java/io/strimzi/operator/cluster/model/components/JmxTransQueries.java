/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.components;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class used to create the JmxTrans queries and which remote hosts to output the results to
 * obj: references what Kafka MBean to reference
 * attr: an array of what MBean properties to target
 * outputDefinitionTemplates: Which hosts and how to output to the hosts
 */
public class JmxTransQueries implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Kafka MBean object whch should be queried
     */
    private String obj;

    /**
     * MBean attributes which should be queried
     */
    private List<String> attr;

    /**
     * Output writer
     */
    private List<JmxTransOutputWriter> outputWriters;

    /**
     * @return  The Kafka MBean which will be queried
     */
    public String getObj() {
        return obj;
    }

    /**
     * Sets the Kafka MBean which will be queried
     *
     * @param obj   Name of the Kafka MBean to query
     */
    public void setObj(String obj) {
        this.obj = obj;
    }

    /**
     * @return  Attributes from the MBean which should be queried
     */
    public List<String> getAttr() {
        return attr;
    }

    /**
     * Sets the MBean attributes which should be queried
     *
     * @param attr  List of attributes
     */
    public void setAttr(List<String> attr) {
        this.attr = attr;
    }

    /**
     * @return  List of output writers
     */
    public List<JmxTransOutputWriter> getOutputWriters() {
        return outputWriters;
    }

    /**
     * Sets the output writers
     *
     * @param outputDefinitionTemplates     List with output writers
     */
    public void setOutputWriters(List<JmxTransOutputWriter> outputDefinitionTemplates) {
        this.outputWriters = outputDefinitionTemplates;
    }
}
