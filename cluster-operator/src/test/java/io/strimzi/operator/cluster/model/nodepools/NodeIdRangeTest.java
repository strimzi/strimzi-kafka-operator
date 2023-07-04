/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NodeIdRangeTest {
    private static final Set<String> VALID_RANGES = Set.of(
            "[5]",
            "[5-10]",
            "[ 5 ]",
            "[ 5 - 10    ]",
            "[0, 1, 2]",
            "[0,1,2]",
            "[5,7-9]",
            "[5 , 7-9, 13]",
            "[1874-1919]",
            "[0-999, 10000-10999, 20000-20999]"
    );

    private static final Set<String> INVALID_RANGES = Set.of(
            "[]",
            "[-5]",
            "[-5-10]",
            "[5-]",
            "[5,]",
            "[-10]",
            " 5 - 10 ]",
            "[0, 1, 2",
            "[0,,2]",
            "[0, a, 2]",
            "[5,7---9]",
            "foo, bar",
            "[foo-bar]"
    );

    @Test
    public void testRangeValidation()  {
        VALID_RANGES.forEach(range -> {
            assertThat(NodeIdRange.isValidNodeIdRange(range), is(true));
        });

        INVALID_RANGES.forEach(range -> {
            assertThat(NodeIdRange.isValidNodeIdRange(range), is(false));
        });
    }

    @Test
    public void testParseRange()  {
        VALID_RANGES.forEach(range -> {
            assertThat(NodeIdRange.parseRanges(range), is(notNullValue()));
        });
    }

    @Test
    public void testNodeIdRangeConstructor()   {
        assertDoesNotThrow(() -> new NodeIdRange.IdRange("1"));
        assertDoesNotThrow(() -> new NodeIdRange.IdRange("5-10"));
        assertDoesNotThrow(() -> new NodeIdRange.IdRange("10-5"));
        assertDoesNotThrow(() -> new NodeIdRange.IdRange("10 - 5"));

        NodeIdRange.InvalidNodeIdRangeException ex = assertThrows(NodeIdRange.InvalidNodeIdRangeException.class,
                () -> new NodeIdRange.IdRange(""));
        assertThat(ex.getMessage(), is("Invalid Node ID range (does not match a single node ID or node ID range): "));

        ex = assertThrows(NodeIdRange.InvalidNodeIdRangeException.class,
                () -> new NodeIdRange.IdRange("999999999999999999999999999999999"));
        assertThat(ex.getMessage(), is("Invalid Node ID (not an integer): 999999999999999999999999999999999"));

        ex = assertThrows(NodeIdRange.InvalidNodeIdRangeException.class,
                () -> new NodeIdRange.IdRange("0-999999999999999999999999999999999"));
        assertThat(ex.getMessage(), is("Invalid Node ID (not an integer): 0-999999999999999999999999999999999"));
    }

    @Test
    public void testNextNodeId()    {
        nodeIdRangeTester("", "");
        nodeIdRangeTester("   ", "");
        nodeIdRangeTester("[]", "");
        nodeIdRangeTester("[  ]", "");
        nodeIdRangeTester(" [ ] ", "");
        nodeIdRangeTester("[ 1 ]", "1;");
        nodeIdRangeTester("[ 1, 2, 3 ]", "1;2;3;");
        nodeIdRangeTester("[ 1, 3, 2 ]", "1;3;2;");
        nodeIdRangeTester("[ 1-3 ]", "1;2;3;");
        nodeIdRangeTester("[ 1-3, 10 - 12 ]", "1;2;3;10;11;12;");
        nodeIdRangeTester("[ 1-3, 6, 7, 8, 10 - 12 ]", "1;2;3;6;7;8;10;11;12;");
        nodeIdRangeTester("[ 1-1 ]", "1;");
        nodeIdRangeTester("[ 3-1 ]", "3;2;1;");
        nodeIdRangeTester("[ 1-3, 12 - 10 ]", "1;2;3;12;11;10;");
    }

    private void nodeIdRangeTester(String rangeAnnotation, String expectedOrder)  {
        NodeIdRange range = new NodeIdRange(rangeAnnotation);
        StringBuilder actualOrder = new StringBuilder();

        Integer nextId;
        while ((nextId = range.getNextNodeId()) != null)  {
            actualOrder.append(nextId).append(";");
        }

        assertThat(actualOrder.toString(), is(expectedOrder));
    }

}
