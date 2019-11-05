/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import org.apache.logging.log4j.core.util.CronExpression;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CronTest {

    @Test
    public void testHoursRangeEveryDay() throws ParseException {
        // every second, every minute, hour 14 to 15 every day --> from 14:00 to 15:59
        CronExpression cronExpression = new CronExpression("* * 14-15 * * ?");

        Date d = Date.from(LocalDateTime.of(2018, 11, 26, 13, 59, 0).atZone(ZoneId.systemDefault()).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(false));
        d = Date.from(LocalDateTime.of(2018, 11, 26, 14, 00, 0).atZone(ZoneId.systemDefault()).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(true));
        d = Date.from(LocalDateTime.of(2018, 11, 26, 15, 59, 0).atZone(ZoneId.systemDefault()).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(true));
        d = Date.from(LocalDateTime.of(2018, 11, 26, 16, 00, 1).atZone(ZoneId.systemDefault()).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(false));
    }

    @Test
    public void testHoursRangeOnWeekend() throws ParseException {
        // every second, every minute, hour 14 to 15 on days of the week 1 and 7 --> from 14:00 to 15:59, on Saturday (7) and Sunday (1)
        CronExpression cronExpression = new CronExpression("* * 14-15 ? * 1,7");

        // Saturday November 24th, 2018
        Date d = Date.from(LocalDateTime.of(2018, 11, 24, 14, 00, 0).atZone(ZoneId.systemDefault()).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(true));
        // Sunday November 25th, 2018
        d = Date.from(LocalDateTime.of(2018, 11, 25, 14, 00, 0).atZone(ZoneId.systemDefault()).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(true));
        // the working day Monday to Friday, November 26th to November 30th, 2018
        for (int day = 26; day <= 30; day++) {
            d = Date.from(LocalDateTime.of(2018, 11, day, 14, 00, 0).atZone(ZoneId.systemDefault()).toInstant());
            assertThat(cronExpression.isSatisfiedBy(d), is(false));
        }
    }

    @Test
    public void testUserTimeZoneVsPodTimeZone() throws ParseException {
        // the "user" writes the cron expression in his timezone
        // every second, every minute, hour 14 to 15 every day --> from 14:00 to 15:59
        CronExpression cronExpression = new CronExpression("* * 14-15 * * ?");

        // the pod is running on a "Pacific/Easter" timezone data center, so cron expression needs the right timezone for evaluation
        cronExpression.setTimeZone(TimeZone.getTimeZone(ZoneId.of("Europe/Rome")));

        // it's really 08:00 in "Pacific/Easter" but 14:00 for "user"
        Date d = Date.from(LocalDateTime.of(2018, 11, 26, 8, 00, 0).atZone(ZoneId.of("Pacific/Easter")).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(true));
    }

    @Test
    public void testUtcTimeZone() throws ParseException {
        // the "user" writes the cron expression in UTC timezone, but let's imagine he is in Europe/Rome timezone
        // every second, every minute, hour 15 to 16 every day --> from 15:00 to 15:59
        CronExpression cronExpression = new CronExpression("* * 14-15 * * ?");

        // the pod is running on a "Pacific/Easter" timezone data center, so cron expression needs the right timezone for evaluation
        cronExpression.setTimeZone(TimeZone.getTimeZone("GMT"));

        // it's really 09:00 in "Pacific/Easter" but 15:00 for "user" so 14:00 in UTC
        Date d = Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("Pacific/Easter")).toInstant());
        assertThat(cronExpression.isSatisfiedBy(d), is(true));
    }
}
