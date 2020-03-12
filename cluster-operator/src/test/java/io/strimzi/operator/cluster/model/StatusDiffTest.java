/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StatusDiffTest {
    @Test
    public void testStatusDiff()    {
        ListenerStatus ls1 = new ListenerStatusBuilder()
                .withNewType("plain")
                .withAddresses(new ListenerAddressBuilder()
                        .withHost("my-service.my-namespace.svc")
                        .withPort(9092)
                        .build())
                .build();

        ListenerStatus ls2 = new ListenerStatusBuilder()
                .withNewType("tls")
                .withAddresses(new ListenerAddressBuilder()
                        .withHost("my-service.my-namespace.svc")
                        .withPort(9093)
                        .build())
                .build();

        ListenerStatus ls3 = new ListenerStatusBuilder()
                .withNewType("tls")
                .withAddresses(new ListenerAddressBuilder()
                        .withHost("my-service.my-namespace.svc")
                        .withPort(9094)
                        .build())
                .build();

        Condition condition1 = new ConditionBuilder()
                .withNewLastTransitionTime(ModelUtils.formatTimestamp(new Date()))
                .withNewType("Ready")
                .withNewStatus("True")
                .build();

        Condition condition2 = new ConditionBuilder()
                .withNewLastTransitionTime(ModelUtils.formatTimestamp(new Date()))
                .withNewType("Ready2")
                .withNewStatus("True")
                .build();

        KafkaStatus status1 = new KafkaStatusBuilder()
                .withConditions(condition1)
                .withListeners(ls1)
                .build();

        KafkaStatus status2 = new KafkaStatusBuilder()
                .withConditions(condition1)
                .withListeners(ls1)
                .build();

        KafkaStatus status3 = new KafkaStatusBuilder()
                .withConditions(condition1)
                .withListeners(ls1, ls2)
                .build();

        KafkaStatus status4 = new KafkaStatusBuilder()
                .withConditions(condition1, condition2)
                .withListeners(ls1)
                .build();

        KafkaStatus status5 = new KafkaStatusBuilder()
                .withConditions(condition1)
                .withListeners(ls1, ls3)
                .build();

        KafkaStatus status6 = new KafkaStatusBuilder()
                .withConditions(condition1)
                .withListeners(ls3, ls1)
                .build();

        StatusDiff diff = new StatusDiff(status1, status2);
        assertThat(diff.isEmpty(), is(true));

        diff = new StatusDiff(status1, status3);
        assertThat(diff.isEmpty(), is(false));

        diff = new StatusDiff(status1, status4);
        assertThat(diff.isEmpty(), is(false));

        diff = new StatusDiff(status3, status4);
        assertThat(diff.isEmpty(), is(false));

        diff = new StatusDiff(status3, status5);
        assertThat(diff.isEmpty(), is(false));

        diff = new StatusDiff(status5, status6);
        assertThat(diff.isEmpty(), is(false));
    }

    @Test
    public void testTimestampDiff() throws ParseException {
        ListenerStatus ls1 = new ListenerStatusBuilder()
                .withNewType("plain")
                .withAddresses(new ListenerAddressBuilder()
                        .withHost("my-service.my-namespace.svc")
                        .withPort(9092)
                        .build())
                .build();

        ListenerStatus ls2 = new ListenerStatusBuilder()
                .withNewType("tls")
                .withAddresses(new ListenerAddressBuilder()
                        .withHost("my-service.my-namespace.svc")
                        .withPort(9093)
                        .build())
                .build();

        Condition condition1 = new ConditionBuilder()
                .withNewLastTransitionTime(ModelUtils.formatTimestamp(new Date()))
                .withNewType("Ready")
                .withNewStatus("True")
                .build();

        Condition condition2 = new ConditionBuilder()
                .withNewLastTransitionTime(ModelUtils.formatTimestamp(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2011-01-01 00:00:00")))
                .withNewType("Ready")
                .withNewStatus("True")
                .build();

        KafkaStatus status1 = new KafkaStatusBuilder()
                .withConditions(condition1)
                .withListeners(ls1, ls2)
                .build();

        KafkaStatus status2 = new KafkaStatusBuilder()
                .withConditions(condition2)
                .withListeners(ls1, ls2)
                .build();

        StatusDiff diff = new StatusDiff(status1, status2);
        assertThat(diff.isEmpty(), is(true));
    }
}
