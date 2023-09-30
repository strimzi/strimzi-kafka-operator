/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Holds a range of Node IDs configured by the user. The range can be defined in the Node Pools as annotation in the
 * form such as [0, 1, 10-15, 100-110, 200] which is used to assign new IDs or scale down nodes.
 */
public class NodeIdRange {
    /**
     * Pattern for validating the range of Node IDs preferred by the user for scale-up or scale-down
     */
    private static final String SINGLE_NODE_ID = "([0-9]+)";
    private static final Pattern SINGLE_NODE_ID_PATTERN = Pattern.compile(SINGLE_NODE_ID);

    private static final String RANGE_OF_NODE_IDS = "([0-9]+\\s*-\\s*[0-9]+)";
    private static final Pattern RANGE_OF_NODE_IDS_PATTERN = Pattern.compile(RANGE_OF_NODE_IDS);

    private final static String SINGLE_OR_RANGE = "(" + SINGLE_NODE_ID + "|" + RANGE_OF_NODE_IDS + ")";
    private static final Pattern NODE_ID_RANGE_PATTERN = Pattern.compile("^\\s*\\[(\\s*" + SINGLE_OR_RANGE + "\\s*,)*\\s*" + SINGLE_OR_RANGE + "\\s*]\\s*$");
    private static final Pattern EMPTY_RANGE_PATTERN = Pattern.compile("^\\s*\\[\\s*]\\s*$");

    private final Queue<IdRange> ranges;

    /**
     * Constructs the Node ID Range which can be used to provide
     *
     * @param range     The array with Node ID ranges from Node Pool annotations
     *
     * @throws InvalidNodeIdRangeException  Throws InvalidNodeIdRangeException if the range is invalid
     */
    public NodeIdRange(String range) throws InvalidNodeIdRangeException {
        if (range.isBlank()
                || EMPTY_RANGE_PATTERN.matcher(range).matches())    {
            // The value is completely empty or an empty array
            ranges = new ArrayDeque<>(0);
        } else if (isValidNodeIdRange(range))   {
            ranges = parseRanges(range);
        } else {
            throw new InvalidNodeIdRangeException("Invalid Node ID range: " + range);
        }
    }

    /**
     * Returns the next node ID
     *
     * @return  The next node ID or null if this range was already depleted
     */
    public Integer getNextNodeId()  {
        while (!ranges.isEmpty())   {
            IdRange nextRange = ranges.peek();

            Integer nextNodeId = nextRange.getNextNodeId();
            if (nextNodeId == null) {
                // The range is empty. We remove it.
                ranges.remove();
            } else {
                return nextNodeId;
            }
        }

        // No more ranges
        return null;
    }

    /**
     * Validates a range definition from a Node Pool annotation
     *
     * @param range The full range as defined in the Node Pool annotation
     *
     * @return  True if the range is valid. False otherwise.
     */
    /* test */ static boolean isValidNodeIdRange(String range) {
        return NODE_ID_RANGE_PATTERN.matcher(range).matches();
    }

    /**
     * Parses Node ID range from the Node Pool annotation into the individual sections
     *
     * @param range The full range as defined in the Node Pool annotation
     *
     * @return  Queue with the individual range sections
     */
    /* test */ static Queue<IdRange> parseRanges(String range)    {
        // Remove the starting and closing brackets together with whitespaces
        range = range.replaceAll("^\\s*\\[\\s*", "");
        range = range.replaceAll("\\s*]\\s*$", "");

        return Arrays.stream(range.split("\\s*,\\s*")).map(IdRange::new).collect(Collectors.toCollection(ArrayDeque::new));
    }

    /**
     * Internal class to hold a single node ID or a single range
     */
    /* test */ static class IdRange   {
        private Integer start;
        private Integer end;

        /**
         * Constructs new IdRange
         *
         * @param rangeItem     A single node ID or range
         *
         * @throws InvalidNodeIdRangeException  Throws InvalidNodeIdRangeException when the range is not valid
         */
        public IdRange(String rangeItem) throws InvalidNodeIdRangeException {
            if (SINGLE_NODE_ID_PATTERN.matcher(rangeItem).matches())    {
                try {
                    start = Integer.parseInt(rangeItem);
                    end = null;
                } catch (NumberFormatException e)   {
                    throw new InvalidNodeIdRangeException("Invalid Node ID (not an integer): " + rangeItem, e);
                }
            } else if (RANGE_OF_NODE_IDS_PATTERN.matcher(rangeItem).matches())  {
                String[] startAndEnd = rangeItem.split("\\s*-\\s*");

                if (startAndEnd.length != 2)    {
                    throw new InvalidNodeIdRangeException("Invalid Node ID range (exactly 2 items must be found): " + rangeItem);
                }

                try {
                    start = Integer.parseInt(startAndEnd[0]);
                    end = Integer.parseInt(startAndEnd[1]);
                } catch (NumberFormatException e)   {
                    throw new InvalidNodeIdRangeException("Invalid Node ID (not an integer): " + rangeItem, e);
                }
            } else {
                throw new InvalidNodeIdRangeException("Invalid Node ID range (does not match a single node ID or node ID range): " + rangeItem);
            }
        }

        /**
         * Returns the next node ID
         *
         * @return  The next ID or null if this range was already depleted
         */
        public Integer getNextNodeId()   {
            if (start == null)  {
                // Starts is null => this range is empty -> return null to indicate that this range was exhausted and should be removed
                return null;
            } else if (end == null)    {
                // Start is not null but end is null => this is a single node id -> return start and set it to null since we are empty now
                Integer returnValue = start;
                start = null;
                return returnValue;
            } else {
                // we have a non-empty range
                if (start.equals(end))   {
                    // Start is the end => return start and set it to null since we will be empty soon
                    Integer returnValue = start;
                    start = null;
                    end = null;
                    return returnValue;
                } else if (start > end) {
                    // Start is bigger than end => return start and decrement it (the range is going down)
                    Integer returnValue = start;
                    start--;
                    return returnValue;
                } else {
                    // Start is smaller than end => return start and increment it (the range is going up)
                    Integer returnValue = start;
                    start++;
                    return returnValue;
                }
            }
        }
    }

    /**
     * This exception is thrown when the Node ID range is invalid.
     */
    public static class InvalidNodeIdRangeException extends RuntimeException    {
        /**
         * Constructor
         *
         * @param message   Exception message
         */
        public InvalidNodeIdRangeException(String message) {
            super(message);
        }

        /**
         * Constructor
         *
         * @param message   Exception message
         * @param cause     Cause exception
         */
        public InvalidNodeIdRangeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
