/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SubjectTests {
    @Test
    public void testSubjectToString()   {
        Subject sbj = new Subject();
        sbj.setCommonName("joe");
        assertThat(sbj.toString(), is("/CN=joe"));

        sbj = new Subject();
        sbj.setOrganizationName("MyOrg");
        assertThat(sbj.toString(), is("/O=MyOrg"));

        sbj = new Subject();
        sbj.setCommonName("joe");
        sbj.setOrganizationName("MyOrg");
        assertThat(sbj.toString(), is("/O=MyOrg/CN=joe"));
    }
}
