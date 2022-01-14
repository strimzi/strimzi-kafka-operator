/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.build.kafka.metadata;

public class Main {
    public static void main(String[] args) throws Exception {
        new KafkaConfigModelGenerator().run(args[0]);
        new KafkaTopicConfigModelGenerator().run(args[1]);
    }
}
