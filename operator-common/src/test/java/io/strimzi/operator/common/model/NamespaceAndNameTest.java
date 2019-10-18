/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import org.junit.Assert;
import org.junit.Test;

public class NamespaceAndNameTest {
    @Test
    public void testEquals()   {
        NamespaceAndName original = new NamespaceAndName("namespace1", "name1");

        Assert.assertTrue(original.equals(new NamespaceAndName("namespace1", "name1")));
        Assert.assertFalse(original.equals(new NamespaceAndName("namespace2", "name1")));
        Assert.assertFalse(original.equals(new NamespaceAndName("namespace1", "name2")));
        Assert.assertFalse(original.equals(new NamespaceAndName("namespace2", "name2")));
    }

    @Test
    public void testToString()   {
        NamespaceAndName nan = new NamespaceAndName("namespace1", "name1");

        Assert.assertEquals("namespace1/name1", nan.toString());
    }
}
