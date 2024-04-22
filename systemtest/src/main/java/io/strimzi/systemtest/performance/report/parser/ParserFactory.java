/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

/**
 * Factory class for creating parser objects based on the specified parser type.
 * This class is part of the performance reporting system and is responsible for
 * instantiating specific parser objects according to the provided {@link ParserType}.
 * Each parser type corresponds to a different component or aspect of the system
 * for which performance metrics need to be parsed.
 */
public class ParserFactory {

    /**
     * Creates an instance of {@link BasePerformanceMetricsParser} based on the specified parser type.
     * This method selects the appropriate parser by matching the provided {@link ParserType} to its
     * corresponding parser class. If the parser type is unknown, it throws an IllegalArgumentException.
     *
     * @param type                          The parser type as defined in {@link ParserType},
     *                                      dictating the specific parser implementation to use.
     * @return                              A new instance of a class extending {@link BasePerformanceMetricsParser},
     *                                      tailored to the specified type.
     * @throws IllegalArgumentException     If the provided parser type is not recognized, indicating that a corresponding
     *                                      parser has not been implemented yet.
     */
    public static BasePerformanceMetricsParser createParser(ParserType type) {
        switch (type) {
            case TOPIC_OPERATOR:
                return new TopicOperatorMetricsParser();
            case USER_OPERATOR:
                return new UserOperatorMetricsParser();
            default:
                throw new IllegalArgumentException("Unknown parser type: " + type);
        }
    }
}