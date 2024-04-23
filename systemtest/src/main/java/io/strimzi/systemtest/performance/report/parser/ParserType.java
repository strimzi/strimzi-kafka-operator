/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

/**
 * Enum representing different types of parsers used in the performance reporting.
 * Each enum constant corresponds to a specific type of performance data parser, identified by a unique parser name.
 * This enum facilitates the mapping of string identifiers to specific parser implementations in a type-safe manner.
 */
public enum ParserType {

    TOPIC_OPERATOR("topic-operator"),
    USER_OPERATOR("user-operator");

    private final String parserName;

    ParserType(String parserName) {
        this.parserName = parserName;
    }

    public String getParserName() {
        return this.parserName;
    }

    /**
     * Converts a string identifier into a {@code ParserType} enum constant.
     * This method performs a case-insensitive comparison between the provided string and the names of the available parser types.
     *
     * @param parserName                    The string identifier of the parser type.
     * @return                              The corresponding {@code ParserType} enum constant.
     * @throws IllegalArgumentException     If the string does not correspond to any known parser type.
     */
    public static ParserType fromString(String parserName) {
        for (ParserType type : ParserType.values()) {
            if (type.getParserName().equalsIgnoreCase(parserName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No parser type found: " + parserName);
    }

    @Override
    public String toString() {
        return this.parserName;
    }
}