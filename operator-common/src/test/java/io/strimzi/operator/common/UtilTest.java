/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.common.Util.parseMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilTest {
    @Test
    public void testParseMap() {
        String stringMap = "key1=value1\n" +
                "key2=value2";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", "value2"));
    }

    @Test
    public void testParseMapNull() {
        Map<String, String> m = parseMap(null);
        assertThat(m, aMapWithSize(0));
    }

    @Test
    public void testParseMapEmptyString() {
        Map<String, String> m = parseMap(null);
        assertThat(m, aMapWithSize(0));
    }

    @Test
    public void testParseMapEmptyValue() {
        String stringMap = "key1=value1\n" +
                "key2=";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", ""));
    }

    @Test
    public void testParseMapInvalid() {
        assertThrows(RuntimeException.class, () -> {
            String stringMap = "key1=value1\n" +
                    "key2";

            Map<String, String> m = parseMap(stringMap);
            assertThat(m, aMapWithSize(2));
            assertThat(m, hasEntry("key1", "value1"));
            assertThat(m, hasEntry("key2", ""));
        });
    }

    @Test
    public void testParseMapValueWithEquals() {
        String stringMap = "key1=value1\n" +
                "key2=value2=value3";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", "value2=value3"));
    }

    @Test
    public void testMaskedPasswords()   {
        String noPassword = "SOME_VARIABLE";
        String passwordAtTheEnd = "SOME_PASSWORD";
        String passwordInTheMiddle = "SOME_PASSWORD_TO_THE_BIG_SECRET";

        assertThat(Util.maskPassword(noPassword, "123456"), is("123456"));
        assertThat(Util.maskPassword(passwordAtTheEnd, "123456"), is("********"));
        assertThat(Util.maskPassword(passwordInTheMiddle, "123456"), is("********"));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithNullWindows() {
        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, null, Instant.now());

        assertThat(result, is(true));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithEmptyWindows() {
        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, Collections.emptyList(), Instant.now());

        assertThat(result, is(true));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithMatchingWindow() {
        // Create a time: 2026-05-02 14:30:00 GMT (Friday)
        ZonedDateTime zdt = ZonedDateTime.of(2026, 5, 2, 14, 30, 0, 0, ZoneId.of("GMT"));

        // Cron that matches: every day at 14:30 (sec min hour day month day-of-week)
        List<String> maintenanceWindows = List.of("0 30 14 * * ?");

        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, maintenanceWindows, zdt.toInstant());

        assertThat(result, is(true));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithNonMatchingWindow() {
        // Create a time: 2026-05-02 14:30:00 GMT (Friday)
        ZonedDateTime zdt = ZonedDateTime.of(2026, 5, 2, 14, 30, 0, 0, ZoneId.of("GMT"));

        // Cron that doesn't match: every day at 10:00 (sec min hour day month day-of-week)
        List<String> maintenanceWindows = List.of("0 0 10 * * ?");

        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, maintenanceWindows, zdt.toInstant());

        assertThat(result, is(false));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithMultipleWindowsOneMatches() {
        // Create a time: 2026-05-02 14:30:00 GMT (Friday)
        ZonedDateTime zdt = ZonedDateTime.of(2026, 5, 2, 14, 30, 0, 0, ZoneId.of("GMT"));

        // Multiple windows: first doesn't match, second matches
        List<String> maintenanceWindows = List.of("0 0 10 * * ?", "0 30 14 * * ?", "0 0 20 * * ?");

        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, maintenanceWindows, zdt.toInstant());

        assertThat(result, is(true));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithMultipleWindowsNoneMatch() {
        // Create a time: 2026-05-02 14:30:00 GMT (Friday)
        ZonedDateTime zdt = ZonedDateTime.of(2026, 5, 2, 14, 30, 0, 0, ZoneId.of("GMT"));

        // Multiple windows: none match
        List<String> maintenanceWindows = List.of("0 0 10 * * ?", "0 0 12 * * ?", "0 0 20 * * ?");

        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, maintenanceWindows, zdt.toInstant());

        assertThat(result, is(false));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithInvalidCronExpression() {
        // Invalid cron expression
        List<String> maintenanceWindows = List.of("invalid-cron-expression");

        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, maintenanceWindows, Instant.now());

        assertThat(result, is(false));
    }

    @Test
    public void testMaintenanceTimeWindowsSatisfiedWithInvalidCronInList() {
        // Create a time: 2026-05-02 14:30:00 GMT (Friday)
        ZonedDateTime zdt = ZonedDateTime.of(2026, 5, 2, 14, 30, 0, 0, ZoneId.of("GMT"));

        // First cron is valid, second is invalid - should return false on invalid
        List<String> maintenanceWindows = List.of("0 0 10 * * ?", "invalid-cron");

        boolean result = Util.isMaintenanceTimeWindowsSatisfied(Reconciliation.DUMMY_RECONCILIATION, maintenanceWindows, zdt.toInstant());

        assertThat(result, is(false));
    }
}
