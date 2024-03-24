/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TopicUpsertTest {

    @Test
    void testEquals() {
        // We don't want the nanosStartOffset parameter to be considered for equality
        var up1 = new TopicUpsert(0, "ns", "n", "100100");
        var up2 = new TopicUpsert(1, "ns", "n", "100100");
        var up3 = new TopicUpsert(1, "ns", "different", "100100");
        assertEquals(up1, up2);
        assertNotEquals(up1, up3);
        assertNotEquals(up2, up3);

        assertEquals(up1.hashCode(), up2.hashCode());
        assertNotEquals(up1.hashCode(), up3.hashCode());
        assertNotEquals(up2.hashCode(), up3.hashCode());
    }

}