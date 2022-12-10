/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

/**
 * Utility class for working with Quantities (memory, CPU, ...)
 */
public class Quantities {
    private Quantities() {

    }

    /**
     * Parse a K8S-style representation of a quantity of memory, such as {@code 512Mi},
     * into the equivalent number of bytes represented as a long.
     * @param memory The String representation of the quantity of memory.
     * @return The equivalent number of bytes.
     */
    public static long parseMemory(String memory) {
        boolean seenDot = false;
        boolean seenE = false;
        boolean seenm = false;
        long factor = 1L;
        int end = memory.length();
        for (int i = 0; i < memory.length(); i++) {
            char ch = memory.charAt(i);
            if (ch == 'e') {
                seenE = true;
            } else if (ch == '.') {
                seenDot = true;
            } else if (ch == 'm') {
                seenm = true;
                end = i;
                break;
            } else if (ch < '0' || '9' < ch) {
                end = i;
                factor = memoryFactor(memory.substring(i));
                break;
            }
        }
        long result;
        String numberPart = memory.substring(0, end);
        if (seenDot || seenE) {
            result = (long) (Double.parseDouble(numberPart) * factor);
        } else if (seenm)   {
            result = (long) Math.floor(Double.parseDouble(numberPart) / 1000);
        } else {
            result = Long.parseLong(numberPart) * factor;
        }
        return result;
    }

    private static long memoryFactor(String suffix) {
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

    protected static String formatMemory(long bytes) {
        return Long.toString(bytes);
    }

    /* test */ static String normalizeMemory(String memory) {
        return formatMemory(parseMemory(memory));
    }

    /**
     * Parse a K8S-style representation of a quantity of cpu, such as {@code 1000m},
     * into the equivalent number of "millicpus" represented as an int.
     * @param cpu The String representation of the quantity of cpu.
     * @return The equivalent number of "millicpus".
     */
    public static int parseCpuAsMilliCpus(String cpu) {
        int suffixIndex = cpu.length() - 1;
        try {
            if ("m".equals(cpu.substring(suffixIndex))) {
                return Integer.parseInt(cpu.substring(0, suffixIndex));
            } else {
                return (int) (Double.parseDouble(cpu) * 1000L);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse CPU quantity \"" + cpu + "\"");
        }
    }

    /* test */ static String formatMilliCpu(int milliCpu) {
        if (milliCpu % 1000 == 0) {
            return Long.toString(milliCpu / 1000L);
        } else {
            return Long.toString(milliCpu) + "m";
        }
    }

    /* test */ static String normalizeCpu(String milliCpu) {
        return formatMilliCpu(parseCpuAsMilliCpus(milliCpu));
    }
}
