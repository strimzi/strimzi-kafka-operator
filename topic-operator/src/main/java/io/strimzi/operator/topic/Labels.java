/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A labels of some object held in K8s.
 */
public class Labels {

    private static final Pattern VALUE_PATTERN = Pattern.compile("([a-z0-9A-Z]([a-z0-9A-Z_.-]*[a-z0-9A-Z])?)?");
    private static final Pattern KEY_PATTERN = Pattern.compile("([a-z0-9.-]*/)?([a-z0-9A-Z](?:[a-z0-9A-Z_.-]*[a-z0-9A-Z])?)");
    private static final Pattern COMMA_SPLITTER = Pattern.compile(",");
    private static final Pattern EQUAL_SPLITTER = Pattern.compile("=");

    private final Map<String, String> labels;

    protected Labels(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        this.labels = new HashMap<>(labels.length / 2);
        for (int i = 0; i < labels.length; i += 2) {
            this.labels.put(labels[i], labels[i + 1]);
        }
        checkLabels(this.labels);
    }

    private Labels(Map<String, String> labels) {
        this.labels = labels;
    }

    /** Is the given {@code label} a valid DNS label */
    private static boolean isLabel(String label) {
        return label.matches("[a-z0-9]([a-z0-9-]*[a-z0-9])")
                && label.length() <= 63;
    }

    private static boolean isSubdomain(String subdomain) {
        if (subdomain.length() <= 253) {
            for (String label : subdomain.split("\\.")) {
                if (!isLabel(label)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static void checkLabels(Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            String key = entry.getKey();
            checkLabelKey(key);
            String value = entry.getValue();
            checkLabelValue(value);
        }
    }

    private static void checkLabelValue(String value) {
        if (value.length() > 63) {
            throw new IllegalArgumentException("The label value is too long (63 character max)");
        }
        Matcher matcher = VALUE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("The label value is invalid");
        }
    }

    private static void checkLabelKey(String key) {
        Matcher keyMatcher = KEY_PATTERN.matcher(key);
        if (keyMatcher.matches()) {
            String prefix = keyMatcher.group(1);
            if (prefix != null && !isSubdomain(prefix.substring(0, prefix.length() - 1))) {
                throw new IllegalArgumentException("The label prefix " + prefix + " is not a valid subdomain");
            }
            String name = keyMatcher.group(2);
            if (name.length() > 63) {
                throw new IllegalArgumentException("The label name is too long (63 character max)");
            }
        } else {
            throw new IllegalArgumentException("The label key is invalid");
        }
    }

    /**
     * Parse the given comma-separated list of {@code key=value} labels into a label predicate, for example:
     * <pre><code>
     * app=strimzi,kind=topic
     * </code></pre>
     *
     * @see <a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set">Syntax and character set</a>
     * @param string The string to parse.
     * @return The label predicate
     */
    public static Labels fromString(String string) throws IllegalArgumentException {
        if (string == null || string.equals("")) {
            return new Labels(Collections.emptyMap());
        }
        Matcher m = COMMA_SPLITTER.matcher(string);
        int lastEnd = 0;
        while (m.find()) {
            String pair = string.substring(lastEnd, m.start());
            if (pair.isEmpty()) {
                throw new IllegalArgumentException();
            }
            lastEnd = m.end();
        }
        String lastPair = string.substring(lastEnd);
        if (lastPair.isEmpty()) {
            throw new IllegalArgumentException();
        }

        String[] pairs = COMMA_SPLITTER.split(string.trim());
        Map<String, String> map = new HashMap<>(pairs.length);
        for (String pair : pairs) {
            String[] keyValue = EQUAL_SPLITTER.split(pair, 2);
            if (keyValue.length < 2) {
                throw new IllegalArgumentException("Couldn't parse the label=value pair: " + pair);
            }
            map.put(keyValue[0], keyValue[1]);
        }
        checkLabels(map);
        return new Labels(map);
    }

    /**
     * @return A map of labels
     */
    public Map<String, String> labels() {
        return labels;
    }

    /**
     * @return String form of label
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean f = false;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            if (f) {
                sb.append("&");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            f = true;
        }
        return sb.toString();
    }
}
