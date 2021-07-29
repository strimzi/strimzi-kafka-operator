/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.strimzi.operator.common.model.RestartReason.CA_CERT_REMOVED;
import static io.strimzi.operator.common.model.RestartReason.CA_CERT_RENEWED;
import static io.strimzi.operator.common.model.RestartReason.CLIENT_CA_CERT_KEY_REPLACED;
import static io.strimzi.operator.common.model.RestartReason.JBOD_VOLUMES_CHANGED;
import static io.strimzi.operator.common.model.RestartReason.MANUAL_ROLLING_UPDATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RestartReasonsTest {

    @Test
    void testMultipleExplicitNotesForOneReasonRetainOrdering() {
        RestartReasons reason = new RestartReasons().add(CA_CERT_RENEWED, "Cert 1 replaced")
                                                    .add(CA_CERT_RENEWED, "Cert 2 replaced")
                                                    .add(CA_CERT_RENEWED, "Cert 3 replaced");

        List<String> expected = List.of("Cert 1 replaced", "Cert 2 replaced", "Cert 3 replaced");
        assertThat(reason.getAllReasonNotes(), is(expected));
    }

    @Test
    void testMultipleExplicitNotesForOneReasonAvoidDuplication() {
        RestartReasons reason = new RestartReasons().add(CA_CERT_RENEWED, "Cert 1 replaced")
                                                    .add(CA_CERT_RENEWED, "Cert 1 replaced")
                                                    .add(CA_CERT_RENEWED, "Cert 1 replaced");

        List<String> expected = List.of("Cert 1 replaced");
        assertThat(reason.getAllReasonNotes(), is(expected));
    }

    @Test
    void testNoteForReasonUsesDefaultWhenNoExplicitNote() {
        RestartReasons reasons = new RestartReasons().add(JBOD_VOLUMES_CHANGED);
        assertThat(reasons.getNoteFor(JBOD_VOLUMES_CHANGED), is(JBOD_VOLUMES_CHANGED.getDefaultNote()));
    }

    @Test
    void testNoteForReasonUsesExplicitWhenProvided() {
        RestartReasons reasons = new RestartReasons().add(JBOD_VOLUMES_CHANGED, "Soon I'm going to be a Jedi");
        assertThat(reasons.getNoteFor(JBOD_VOLUMES_CHANGED), is("Soon I'm going to be a Jedi"));
    }

    @Test
    void testNoteForReasonWithMultipleExplicitNotesConcatenates() {
        RestartReasons reasons = new RestartReasons().add(JBOD_VOLUMES_CHANGED, "Well I know he built C3PO")
                                                     .add(JBOD_VOLUMES_CHANGED, "And I've heard how fast his pod can go")
                                                     .add(JBOD_VOLUMES_CHANGED, "And we were broke it's true");

        String expected = "Well I know he built C3PO, And I've heard how fast his pod can go, And we were broke it's true";
        assertThat(reasons.getNoteFor(JBOD_VOLUMES_CHANGED), is(expected));
    }

    @Test
    void testAllReasonNotesMergesDefaultsAndExplicitNotes() {
        String explicitNote1 = "They see me mowing my front lawn";
        String explicitNote2 = "I know they're all thinking";
        String explicitNote3 = "I'm so white and nerdy";

        RestartReasons reasons = new RestartReasons().add(CA_CERT_REMOVED)
                                                     .add(CLIENT_CA_CERT_KEY_REPLACED)
                                                     .add(MANUAL_ROLLING_UPDATE, explicitNote1)
                                                     .add(MANUAL_ROLLING_UPDATE, explicitNote2)
                                                     .add(JBOD_VOLUMES_CHANGED, explicitNote3);

        Set<String> expected = Set.of(CA_CERT_REMOVED.getDefaultNote(),
                                      CLIENT_CA_CERT_KEY_REPLACED.getDefaultNote(),
                                      explicitNote1,
                                      explicitNote2,
                                      explicitNote3);

        assertThat(new HashSet<>(reasons.getAllReasonNotes()), is(expected));
    }
}