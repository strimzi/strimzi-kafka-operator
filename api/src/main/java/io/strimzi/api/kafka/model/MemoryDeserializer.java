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
 * Converts Strings in Kubernetes memory syntax to/from bytes expressed as a long.
 * @see <a href="https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-memory">Kubernetes docs</a>
 */
public class MemoryDeserializer extends StdScalarDeserializer<Long> {

    private static final long serialVersionUID = 1L;

    public MemoryDeserializer() {
        this(Long.class);
    }

    protected MemoryDeserializer(Class vc) {
        super(vc);
    }

    protected MemoryDeserializer(JavaType valueType) {
        super(valueType);
    }

    protected MemoryDeserializer(StdScalarDeserializer src) {
        super(src);
    }

    @Override
    public Long deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        switch (p.getCurrentTokenId()) {
            case JsonTokenId.ID_NUMBER_INT:
                switch (p.getNumberType()) {
                    case INT:
                    case LONG:
                        return p.getLongValue();
                }
                break;
            case JsonTokenId.ID_STRING:
                String text = p.getText().trim();
                try {
                    return parse(text);
                } catch (IllegalArgumentException e) {
                    return (Long) ctxt.handleWeirdStringValue(_valueClass, text,
                            "not a valid representation");
                }
        }
        return (Long) ctxt.handleUnexpectedToken(_valueClass, p);
    }

    /**
     * Parse a K8S-style representation of a quantity of memory, such as {@code 512Mi},
     * into the equivalent number of bytes represented as a long.
     * @param memory The String representation of the quantity of memory.
     * @return The equivalent number of bytes.
     */
    public static long parse(String memory) {
        boolean seenE = false;
        long factor = 1L;
        int end = memory.length();
        for (int i = 0; i < memory.length(); i++) {
            char ch = memory.charAt(i);
            if (ch == 'e') {
                seenE = true;
            } else if (ch < '0' || '9' < ch) {
                end = i;
                factor = factor(memory.substring(i));
                break;
            }
        }
        long result;
        if (seenE) {
            result = (long) Double.parseDouble(memory);
        } else {
            result = Long.parseLong(memory.substring(0, end)) * factor;
        }
        return result;
    }

    private static long factor(String suffix) {
        long factor;
        switch (suffix) {
            case "E":
                factor = 1_000L * 1_000L * 1_000L * 1_000 * 1_000L;
                break;
            case "T":
                factor = 1_000L * 1_000L * 1_000L * 1_000;
                break;
            case "G":
                factor = 1_000L * 1_000L * 1_000L;
                break;
            case "M":
                factor = 1_000L * 1_000L;
                break;
            case "K":
                factor = 1_000L;
                break;
            case "Ei":
                factor = 1_024L * 1_024L * 1_024L * 1_024L * 1_024L;
                break;
            case "Ti":
                factor = 1_024L * 1_024L * 1_024L * 1_024L;
                break;
            case "Gi":
                factor = 1_024L * 1_024L * 1_024L;
                break;
            case "Mi":
                factor = 1_024L * 1_024L;
                break;
            case "Ki":
                factor = 1_024L;
                break;
            default:
                throw new IllegalArgumentException("Invalid memory suffix: " + suffix);
        }
        return factor;
    }

    public static String format(long bytes) {
        if (bytes == 0) {
            return "0";
        }
        long x;
        int i;
        x = bytes;
        i = -1;
        while ((x % 1000L) == 0L && i < 4) {
            i++;
            x = x / 1000L;
        }
        if (i >= 0) {
            return x + new String[] {"k", "M", "G", "T", "E"}[i];
        }
        x = bytes;
        i = -1;
        while ((x % 1024L) == 0L && i < 4) {
            i++;
            x = x / 1024L;
        }
        if (i >= 0) {
            return x + new String[]{"ki", "Mi", "Gi", "Ti", "Ei"}[i];
        }
        return Long.toString(bytes);
    }
}

