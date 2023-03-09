/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EitherTest {

    private final Either<Integer, String> left = Either.<Integer, String>ofLeft(1);
    private final Either<Integer, String> right = Either.<Integer, String>ofRight("A");

    @Test
    void testAccessors() {
        assertTrue(left.isLeft());
        assertFalse(right.isLeft());
        assertEquals(1, left.left());
        assertThrows(IllegalStateException.class, () -> left.right());
        assertEquals("A", right.right());
        assertThrows(IllegalStateException.class, () -> right.left());
    }

    @Test
    void testEquals() {
        assertEquals(Either.<Integer, String>ofLeft(1), left);
        assertNotEquals(Either.<Integer, String>ofLeft(2), left);
        assertEquals(Either.<Integer, String>ofLeft(1).hashCode(), left.hashCode());
        assertNotEquals(Either.<Integer, String>ofLeft(2).hashCode(), left.hashCode());
        assertTrue(left.isLeftEqual(1));
        assertFalse(left.isLeftEqual(2));

        assertEquals(Either.<Integer, String>ofRight("A"), right);
        assertNotEquals(Either.<Integer, String>ofRight("B"), right);
        assertEquals(Either.<Integer, String>ofRight("A").hashCode(), right.hashCode());
        assertNotEquals(Either.<Integer, String>ofRight("B").hashCode(), right.hashCode());
        assertFalse(right.isLeftEqual(1));
        assertFalse(right.isLeftEqual(2));
    }

    @Test
    void testMapLeft() {
        var mappedLeft = left.mapLeft(i -> i + 1);
        var mappedRight = right.mapLeft(i -> i + 1);
        assertEquals(Either.<Integer, String>ofLeft(2), mappedLeft);
        assertEquals(right, mappedRight);
    }

    @Test
    void testFlatMapLeft() {
        var mappedLeft = left.flatMapLeft(i -> Either.ofLeft(i + 1));
        var mappedRight = right.flatMapLeft(i -> Either.<Integer, String>ofRight("B"));
        assertEquals(Either.<Integer, String>ofLeft(2), mappedLeft);
        assertEquals(right, mappedRight);
    }

    @Test
    void testToString() {
        assertEquals("Left(1)", left.toString());
        assertEquals("Right(A)", right.toString());
    }

}