/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PlatformFeaturesAvailabilityTest {

    @Test
    public void basicTest() {
        boolean isOpenshift = true;
        String response = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(isOpenshift, response);
        assertTrue(pfa.isOpenshift());
        assertEquals(9, pfa.getMinorVersion());
        assertEquals(1, pfa.getMajorVersion());
    }

    @Test
    public void newerVersionTest() {
        boolean isOpenshift = true;
        String response = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(isOpenshift, response);

        assertTrue(pfa.isEqualOrNewerVersionThan(0, 8));
        assertTrue(pfa.isEqualOrNewerVersionThan(0, 9));
        assertTrue(pfa.isEqualOrNewerVersionThan(0, 10));

        assertTrue(pfa.isEqualOrNewerVersionThan(1, 8));
        assertTrue(pfa.isEqualOrNewerVersionThan(1, 9));
        assertFalse(pfa.isEqualOrNewerVersionThan(1, 10));

        assertFalse(pfa.isEqualOrNewerVersionThan(2, 8));
        assertFalse(pfa.isEqualOrNewerVersionThan(2, 9));
        assertFalse(pfa.isEqualOrNewerVersionThan(2, 10));


        assertTrue(pfa.isEqualOrNewerVersionThan("0.8"));
        assertTrue(pfa.isEqualOrNewerVersionThan("0.9"));
        assertTrue(pfa.isEqualOrNewerVersionThan("0.10"));

        assertTrue(pfa.isEqualOrNewerVersionThan("1.8"));
        assertTrue(pfa.isEqualOrNewerVersionThan("1.9"));
        assertFalse(pfa.isEqualOrNewerVersionThan("1.10"));

        assertFalse(pfa.isEqualOrNewerVersionThan("2.8"));
        assertFalse(pfa.isEqualOrNewerVersionThan("2.9"));
        assertFalse(pfa.isEqualOrNewerVersionThan("2.10"));


        try {
            assertFalse(pfa.isEqualOrNewerVersionThan("2"));
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
            } else {
                fail();
            }
        }

        try {
            assertFalse(pfa.isEqualOrNewerVersionThan("f.10"));
        } catch (Exception e) {
            if (e instanceof NumberFormatException) {
            } else {
                fail();
            }
        }
    }

    @Test
    public void networkPolicyTest() {
        boolean isOpenshift = true;
        String response = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        String response2 = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"11\",\n" +
                "  \"gitVersion\": \"v1.11.1+deadbeef\",\n" +
                "  \"gitCommit\": \"deadbeef\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2019-06-24T01:54:00Z\",\n" + // manually edited
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(isOpenshift, response);
        assertFalse(pfa.isNetworkPolicyAvailable());
        PlatformFeaturesAvailability pfa2 = new PlatformFeaturesAvailability(isOpenshift, response2);
        assertTrue(pfa2.isNetworkPolicyAvailable());
    }
}
