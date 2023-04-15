/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
import io.strimzi.annotations.TestDoc;
import org.junit.jupiter.api.Test;

public class CompletelyUnrelatedClass {
    @Test
    @TestDoc(
        description = @TestDoc.Desc("Test checking that we are able to do message exchange without auth"),
        steps = {
            @TestDoc.Step(value = "Deploy Kafka with 3 Kafka and 3 ZK pods and wait for readiness", expected = "Kafka cluster is deployed"),
            @TestDoc.Step(value = "Deploy KafkaTopic inside the created Kafka cluster", expected = "KafkaTopic is deployed"),
            @TestDoc.Step(value = "Using test-clients, do the message exchange", expected = "Messages are successfully sent and received"),
            @TestDoc.Step(value = "Check that Kafka bootstrap service contains expected values", expected = "Kafka bootstrap service contains expected values")
        },
        usecases = {
            @TestDoc.Usecase(id = "message-transmission")
        }
    )
    void testSendMessagesPlainAnonymous() {
    }

    @Test
    @TestDoc(
        description = @TestDoc.Desc("lorem ipsum"),
        steps = {
            @TestDoc.Step(value = "lorem ipsum", expected = "lorem ipsum"),
            @TestDoc.Step(value = "lorem ipsum", expected = "lorem ipsum"),
            @TestDoc.Step(value = "lorem ipsum", expected = "lorem ipsum"),
            @TestDoc.Step(value = "lorem ipsum", expected = "lorem ipsum")
        },
        usecases = {
            @TestDoc.Usecase(id = "message-transmission"),
            @TestDoc.Usecase(id = "lorem-ipsum")
        }
    )
    void testRemoveMeAfterTheEnd() {
    }
}
