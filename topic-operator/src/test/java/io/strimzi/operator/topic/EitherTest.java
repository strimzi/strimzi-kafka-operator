/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EitherTest {

    private final Either<String, Integer> right = Either.ofRight(1);
    private final Either<String, Integer> left = Either.ofLeft("A");

    @Test
    void testAccessors() {
        assertTrue(right.isRight());
        assertFalse(left.isRight());
        assertEquals(1, right.right());
        assertThrows(IllegalStateException.class, right::left);
        assertEquals("A", left.left());
        assertThrows(IllegalStateException.class, left::right);
    }

    @Test
    void testEquals() {
        assertEquals(Either.<String, Integer>ofRight(1), right);
        assertNotEquals(Either.<String, Integer>ofRight(2), right);
        assertEquals(Either.<String, Integer>ofRight(1).hashCode(), right.hashCode());
        assertNotEquals(Either.<String, Integer>ofRight(2).hashCode(), right.hashCode());
        assertTrue(right.isRightEqual(1));
        assertFalse(right.isRightEqual(2));

        assertEquals(Either.<String, Integer>ofLeft("A"), left);
        assertNotEquals(Either.<String, Integer>ofLeft("B"), left);
        assertEquals(Either.<String, Integer>ofLeft("A").hashCode(), left.hashCode());
        assertNotEquals(Either.<String, Integer>ofLeft("B").hashCode(), left.hashCode());
        assertFalse(left.isRightEqual(1));
        assertFalse(left.isRightEqual(2));
    }

    @Test
    void testMapLeft() {
        var mappedRight = right.mapRight(i -> i + 1);
        var mappedLeft = left.mapRight(i -> i + 1);
        assertEquals(Either.<String, Integer>ofRight(2), mappedRight);
        assertEquals(left, mappedLeft);
    }

    @Test
    void testFlatMapLeft() {
        var mappedRight = right.flatMapRight(i -> Either.ofRight(i + 1));
        var mappedLeft = left.flatMapRight(i -> Either.ofLeft("B"));
        assertEquals(Either.ofRight(2), mappedRight);
        assertEquals(left, mappedLeft);
    }

    @Test
    void testToString() {
        assertEquals("Right(1)", right.toString());
        assertEquals("Left(A)", left.toString());
    }

}