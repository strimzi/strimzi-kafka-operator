/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

import java.io.IOException;

/**
 * Converts Strings in Kubernetes cpu syntax to/from "millicpus" expressed as an int.
 * Keeping the quantity as millicpus internally avoids the need for floating point arithmetic.
 * @see <a href="https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-cpu">Kubernetes docs</a>
 */
public class MilliCpuDeserializer extends StdScalarDeserializer<Integer> {

    private static final long serialVersionUID = 1L;

    public MilliCpuDeserializer() {
        this(Long.class);
    }

    protected MilliCpuDeserializer(Class vc) {
        super(vc);
    }

    protected MilliCpuDeserializer(JavaType valueType) {
        super(valueType);
    }

    protected MilliCpuDeserializer(StdScalarDeserializer src) {
        super(src);
    }

    @Override
    public Integer deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        switch (p.getCurrentTokenId()) {
            case JsonTokenId.ID_NUMBER_INT:
                switch (p.getNumberType()) {
                    case INT:
                        // interpret a json int as that many cpus, not millicpus
                        return p.getIntValue() * 1000;
                }
                break;
            case JsonTokenId.ID_STRING:
                String text = p.getText().trim();
                try {
                    return parse(text);
                } catch (IllegalArgumentException e) {
                    return (Integer) ctxt.handleWeirdStringValue(_valueClass, text,
                            "not a valid representation");
                }
        }
        return (Integer) ctxt.handleUnexpectedToken(_valueClass, p);
    }

    /**
     * Parse a K8S-style representation of a quantity of cpu, such as {@code 1000m},
     * into the equivalent number of "millicpus" represented as an int.
     * @param cpu The String representation of the quantity of cpu.
     * @return The equivalent number of "millicpus".
     */
    public static int parse(String cpu) {
        int suffixIndex = cpu.length();
        int factor = 1000;
        for (int i = 0; i < cpu.length(); i++) {
            char ch = cpu.charAt(i);
            if (ch < '0' || '9' < ch) {
                suffixIndex = i;
                if ("m".equals(cpu.substring(i))) {
                    factor = 1;
                    break;
                } else if (cpu.substring(i).startsWith(".")) {
                    return (int) (Double.parseDouble(cpu) * 1000L);
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }
        return factor * Integer.parseInt(cpu.substring(0, suffixIndex));
    }

    public static String format(int milliCpu) {
        if (milliCpu % 1000 == 0) {
            return Long.toString(milliCpu / 1000L);
        } else {
            return Long.toString(milliCpu) + "m";
        }
    }
}
