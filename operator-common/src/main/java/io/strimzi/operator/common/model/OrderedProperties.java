/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A Set of insertion ordered Name/Value pairs.
 */
public class OrderedProperties {

    private final Map<String, String> pairs = new LinkedHashMap<>();

    /**
     * Filter key/value pairs
     *
     * @param filter The predicate which determines whether key/value pair will be retained.  The
     * predicate is invoked with the key.  The pair is removed if the predicate returns true.
     * @return this instance for chaining
     */
    public OrderedProperties filter(Predicate<String> filter) {
        for (Iterator<Entry<String, String>> it = pairs.entrySet().iterator(); it.hasNext(); ) {
            if (filter.test(it.next().getKey())) {
                it.remove();
            }
        }
        return this;
    }

    /**
     * Add Key/Value pairs
     *
     * @param additionalPairs Key/Value pairs to add to current set (may be null)
     * @return this instance for chaining
     */
    public OrderedProperties addMapPairs(Map<String, String> additionalPairs) {
        addIterablePairs(additionalPairs.entrySet());
        return this;
    }

    /**
     * Converts the values from the Iterable into String values and adds the pairs to the current set.
     *
     * @param iterable Key/Value pairs These are Map.Entry&lt;?, ?&gt; due to Properties extending Hashtable&lt;?, ?&gt;
     * @return this instance for chaining
     * @throws InvalidConfigParameterException if value is null or is of an unsupported type
     */
    public OrderedProperties addIterablePairs(Iterable<? extends Map.Entry<String, ?>> iterable) {
        for (Map.Entry<String, ?> entry : iterable) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                pairs.put(key, (String) value);
            } else if (value instanceof Integer || value instanceof Long || value instanceof Boolean
                || value instanceof Double || value instanceof Float) {
                pairs.put(key, String.valueOf(value));
            } else if (value == null) {
                throw new InvalidConfigParameterException(key, "A null value is not allowed for this key");
            } else {
                throw new InvalidConfigParameterException(key,
                    "Unsupported type " + value.getClass() + " in configuration for this key");
            }
        }
        return this;
    }

    /**
     * Parse key/value pairs and add to the current pair set.
     * @param keyValuePairs Pairs in key=value format, pairs separated by newlines
     * @throws InvalidConfigParameterException if value is null
     * @return this instance for chaining
     */
    public OrderedProperties addStringPairs(String keyValuePairs) {
        PropertiesReader reader = new PropertiesReader(pairs);
        reader.read(keyValuePairs);
        return this;
    }

    /**
     * Parse key/value pairs and add to the current pair set.
     * Read map values from an InputStream.  The InputStream is closed after all values are read.
     * @param is The UTF-8 input stream containing name=value pairs separated by newlines.
     *
     * @return this instance for chaining
     * @throws IOException If the input stream could not be read.
     */
    public OrderedProperties addStringPairs(InputStream is) throws IOException {
        new PropertiesReader(pairs).read(is);
        return this;
    }

    /**
     * Add a key/value pair
     * @param key The key
     * @param value The value
     * @return this instance for chaining
     */
    public OrderedProperties addPair(String key, String value) {
        pairs.put(key, value);
        return this;
    }

    /**
     * Generate a string with pairs formatted as key=value separated by newlines.  This string is
     * expected to be used as an environment variable in a bash script and subsequently used in a bash
     * <a href="https://www.tldp.org/LDP/abs/html/here-docs.html">here document</a> to create a
     * properties file.
     *
     * @return String with one or more lines containing key=value pairs with the configuration options.
     */
    public String asPairs() {
        return asPairsWithComment(null);
    }

    /**
     * Generate a string with pairs formatted as key=value separated by newlines.  This string is
     * expected to be used as an environment variable in a bash script and subsequently used in a bash
     * <a href="https://www.tldp.org/LDP/abs/html/here-docs.html">here document</a> to create a
     * properties file.
     *
     * @param comment A comment to be prepended to the output, or null for no comment.
     * @return String with one or more lines containing key=value pairs with the configuration options.
     */
    public String asPairsWithComment(String comment) {
        return new PropertiesWriter(pairs).writeString(comment);
    }

    /**
     * Return a Map view of the underlying key-value pairs. Any changes to this map will be reflected in
     * the value returned by subsequent calls.
     *
     * @return A map of keys to values.
     */
    public Map<String, String> asMap() {
        return pairs;
    }

    @Override
    public int hashCode() {
        return pairs.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OrderedProperties && pairs.equals(((OrderedProperties) obj).pairs);
    }

    @Override
    public String toString() {
        return "OrderedProperties{" +
                "pairs=" + pairs +
                '}';
    }

    /**
     * Read values into a Map&lt;String, String&gt; from a Properties compatible format.
     * An instance of this class is not thread-safe; the result of invoking any of the
     * read methods simultaneously is not defined.
     */
    static private class PropertiesReader {
        private static final int EOF = -1;
        private static final int NO_CHAR = -2;

        private final Map<String, String> map;
        private BufferedReader bufferedReader;
        private int peekChar = NO_CHAR;

        public PropertiesReader(Map<String, String> map) {
            this.map = map;
        }

        /**
         * Read map values from a String.
         *
         * @param keyValuePairs String containing name=value pairs separated by newlines.
         */
        public void read(String keyValuePairs) {
            try {
                read(new StringReader(keyValuePairs));
            } catch (IOException e) {
                throw new IllegalStateException("StringReader should not cause IOException", e);
            }
        }

        /**
         * Read map values from an InputStream.  The InputStream is closed after all values are read.
         *
         * @param is The UTF-8 input stream containing name=value pairs separated by newlines.
         */
        public void read(InputStream is) throws IOException {
            read(new InputStreamReader(is, StandardCharsets.UTF_8));
        }

        /**
         * Read map values from a Reader.  The Reader is closed after all values are read.
         *
         * @param reader Reader containing name=value pairs separated by newlines.
         * @throws IOException when read or close fails
         */
        public void read(Reader reader) throws IOException {
            try (BufferedReader bufferedReader = new BufferedReader(reader)) {
                read(bufferedReader);
            }
        }

        /**
         * Read map values from a BufferedReader.  The BufferedReader is not closed after all values are
         * read.
         *
         * @param bufferedReader Reader containing name=value pairs separated by newlines.
         * @throws IOException when read or close fails
         */
        public void read(BufferedReader bufferedReader) throws IOException {
            this.bufferedReader = bufferedReader;
            for (; ; ) {
                ignoreWhitespace(true);
                if (peekChar == EOF) {
                    return;
                }

                if (isComment()) {
                    ignoreToEndOfLine();
                    continue;
                }

                String key = readToken(true);

                ignoreWhitespace(false);
                if (isKeySeparator()) {
                    peekChar = NO_CHAR;
                    ignoreWhitespace(false);
                }

                String value = readToken(false);
                map.put(key, value);
            }
        }

        private String readToken(boolean breakOnKeySeparator) throws IOException {
            StringBuilder sb = new StringBuilder();
            for (; ; ) {
                switch (peekChar) {
                    case '\t':
                    case '\f':
                    case ' ':
                    case ':':
                    case '=':
                        if (!breakOnKeySeparator) {
                            break;
                        }
                    case '\r':
                    case '\n':
                    case EOF:
                        return sb.toString();
                    case '\\':
                        sb.append(readEscape());
                        continue;
                }
                sb.append((char) peekChar);
                peekChar = bufferedReader.read();
            }
        }

        private String readEscape() throws IOException {
            int ec = bufferedReader.read();
            String rc;
            switch (ec) {
                case '\r':
                case '\n':
                    peekChar = bufferedReader.read();
                    ignoreWhitespace(true);
                    return "";
                case 'u':
                    rc = readUnicode();
                    break;
                case 't':
                    rc = "\t";
                    break;
                case 'f':
                    rc = "\f";
                    break;
                case 'r':
                    rc = "\r";
                    break;
                case 'n':
                    rc = "\n";
                    break;
                default:
                    rc = Character.toString((char) ec);
                    break;
            }
            peekChar = bufferedReader.read();
            return rc;
        }

        private String readUnicode() throws IOException {
            int sum = 0;
            for (int h = 0; h < 4; ++h) {
                int hexIt;
                peekChar = bufferedReader.read();
                if (peekChar >= '0' && peekChar <= '9') {
                    hexIt = peekChar - '0';
                } else if (peekChar >= 'a' && peekChar <= 'f') {
                    hexIt = peekChar - 'a' + 10;
                } else if (peekChar >= 'A' && peekChar <= 'F') {
                    hexIt = peekChar - 'A' + 10;
                } else {
                    throw new IllegalArgumentException("Malformed \\uxxxx encoding");
                }
                sum = sum * 16 + hexIt;
            }
            return Character.toString((char) sum);
        }

        /*
         * On entry, peekChar is at comment char
         * On exit, peekChar is at newline or EOF
         */
        private void ignoreToEndOfLine() throws IOException {
            for (; ; ) {
                peekChar = bufferedReader.read();
                if (isEol()) {
                    break;
                }
            }
        }

        /*
         * On entry, peekChar is at NO_CHAR or after key
         * On exit, peekChar is at non-whitespace, newline or EOF
         */
        private void ignoreWhitespace(boolean includeNewLine) throws IOException {
            for (; ; peekChar = bufferedReader.read()) {
                switch (peekChar) {
                    case '\r':
                    case '\n':
                        if (!includeNewLine) {
                            return;
                        }
                    case '\t':
                    case '\f':
                    case ' ':
                    case NO_CHAR:
                        continue;
                    default:
                        return;
                }
            }
        }

        private boolean isComment() {
            switch (peekChar) {
                case '!':
                case '#':
                    return true;
            }
            return false;
        }

        private boolean isKeySeparator() {
            switch (peekChar) {
                case '=':
                case ':':
                    return true;
            }
            return false;
        }

        private boolean isEol() {
            switch (peekChar) {
                case '\r':
                case '\n':
                case EOF:
                    return true;
            }
            return false;
        }
    }


    /**
     * Write values from a Map&lt;String, String&gt; in a Properties compatible format.
     * Unlike Properties.store(), no comments are added to beginning of output and escapes sequences are
     * minimized.
     *
     * Each line contains name=value.
     * Any '=', ':', ' ', '\t', '\f', or '\n' in the name will be escaped with '\'.
     * Any '\r', '\n' in the value will be escaped with '\'.
     * Any leading ' ', '\t', '\f' in value will be escaped with '\'.
     *
     * An instance of this class is thread-safe as long as iterating the wrapped map is thread-safe.
     */
    static private class PropertiesWriter {
        public static final Pattern LINE_SPLITTER = Pattern.compile("[\\r\\n]+");
        private final Map<String, String> map;
        private BufferedWriter bufferedWriter;

        public PropertiesWriter(Map<String, String> map) {
            this.map = map;
        }

        /**
         * Write map values to a String.
         *
         * @return A String containing name=value pairs separated by newlines.
         * @param comment A comment to be prepended to the output, or null for no comment.
         */
        public String writeString(String comment) {
            StringWriter sw = new StringWriter();
            try {
                write(sw, comment);
            } catch (IOException e) {
                throw new IllegalStateException("StringWriter should not cause IOException", e);
            }
            return sw.toString();
        }

        /**
         * Write map values to Writer.  The Writer is not closed.
         *
         * @param writer Writer to write values to.
         * @param comment A comment to be prepended to the output, or null for no comment.
         */
        public void write(Writer writer, String comment) throws IOException {
            write(new BufferedWriter(writer), comment);
        }

        /**
         * Write map values to BufferedWriter.  The BufferedWriter is not closed.
         *
         * @param bufferedWriter BufferedWriter to write values to.
         * @param comment A comment to be prepended to the output, or null for no comment.
         */
        public void write(BufferedWriter bufferedWriter, String comment) throws IOException {
            this.bufferedWriter = bufferedWriter;
            if (comment != null) {
                writeComment(bufferedWriter, comment);
            }

            for (Map.Entry<String, String> entry : map.entrySet()) {
                escapeKey(entry.getKey());
                bufferedWriter.append('=');
                escapeValue(entry.getValue());
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        }

        /**
         * Write comment to a Writer, handling newlines embedded in the comment
         * @param bufferedWriter BufferedWriter to write.
         * @param comment A comment to be written
         * @throws IOException
         */
        private static void writeComment(BufferedWriter bufferedWriter, String comment) throws IOException {
            for (String line : LINE_SPLITTER.split(comment)) {
                bufferedWriter.write("# ");
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
        }

        /**
         * A properties key may not contain '=', ':', ' ', '\t', '\f', or '\n'.
         * Escape the key
         */
        private void escapeKey(String k) throws IOException {
            for (int i = 0; i < k.length(); ++i) {
                char c = k.charAt(i);
                switch (c) {
                    case '\n':
                        bufferedWriter.append("\\n");
                        continue;
                    case '=':
                    case ':':
                    case ' ':
                    case '\t':
                    case '\f':
                    case '\\':
                        bufferedWriter.append('\\');
                }
                bufferedWriter.append(c);
            }
        }

        /**
         * A properties value may not contain '\r', '\n'.  Value may not have leading white space.
         * Escape the value
         */
        private void escapeValue(String v) throws IOException {
            for (int i = 0; i < v.length(); ++i) {
                char c = v.charAt(i);
                switch (c) {
                    case '\r':
                        bufferedWriter.append("\\r");
                        break;
                    case '\n':
                        bufferedWriter.append("\\n");
                        break;
                    case '\\':
                        bufferedWriter.append("\\\\");
                        break;
                    case ' ':
                    case '\t':
                    case '\f':
                        // Value may not have leading white space.
                        if (i == 0) {
                            bufferedWriter.append('\\');
                        }
                    default:
                        bufferedWriter.append(c);
                }
            }
        }
    }
}
