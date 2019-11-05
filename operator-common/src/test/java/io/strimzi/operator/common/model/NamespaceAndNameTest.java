/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class NamespaceAndNameTest {
    @Test
    public void testEquals()   {
        NamespaceAndName original = new NamespaceAndName("namespace1", "name1");

        assertThat(original, is(new NamespaceAndName("namespace1", "name1")));
        assertThat(original, is(not(new NamespaceAndName("namespace2", "name1"))));
        assertThat(original, is(not(new NamespaceAndName("namespace1", "name2"))));
        assertThat(original, is(not(new NamespaceAndName("namespace2", "name2"))));
    }

    @Test
    public void testToString()   {
        NamespaceAndName nan = new NamespaceAndName("namespace1", "name1");

        assertThat(nan.toString(), is("namespace1/name1"));
    }
}
