/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SubjectTests {
    @Test
    public void testSubjectToString()   {
        Subject sbj = new Subject();
        sbj.setCommonName("joe");
        assertEquals("/CN=joe", sbj.toString());

        sbj = new Subject();
        sbj.setOrganizationName("MyOrg");
        assertEquals("/O=MyOrg", sbj.toString());

        sbj = new Subject();
        sbj.setCommonName("joe");
        sbj.setOrganizationName("MyOrg");
        assertEquals("/O=MyOrg/CN=joe", sbj.toString());
    }
}
