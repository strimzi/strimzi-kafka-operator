package io.strimzi.operator.common.operator.resource.publication.kubernetes;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class WorkaroundMicroTimeTest {

    @Test
    void testRoundTrip() {
        ZonedDateTime utc = Instant.parse("2020-01-02T01:15:05.000000Z").atZone(ZoneId.of("UTC"));
        assertThat(new WorkaroundMicroTime(utc).serialise(), is("2020-01-02T01:15:05.000000Z"));
    }

    @Test
    void testNonUtcExpressesTimezoneInFull() {
        //In January, NZ is in daylight savings time, so is UTC+13, as opposed to the usual twelve
        ZonedDateTime utcPlus13 = Instant.parse("2020-01-02T00:15:05.000000Z").atZone(ZoneId.of("NZ"));
        //Sanity check I got that right...
        assertThat(utcPlus13.getHour(), is (13));

        assertThat(new WorkaroundMicroTime(utcPlus13).serialise(), is("2020-01-02T13:15:05.000000+13:00"));
    }

    @Test
    void testMicrosecondsAlwaysExpressed() {
        ZonedDateTime utc = Instant.parse("2020-01-02T01:15:05Z").atZone(ZoneId.of("UTC"));
        assertThat(new WorkaroundMicroTime(utc).serialise(), is("2020-01-02T01:15:05.000000Z"));
    }


}