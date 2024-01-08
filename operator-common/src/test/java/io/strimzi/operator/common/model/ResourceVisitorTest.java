/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Member;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class ResourceVisitorTest {
    @Test
    public void testDoesNotThrow() {
        Kafka k = TestUtils.fromYaml("/example.yaml", Kafka.class, true);
        assertThat(k, is(notNullValue()));
        ResourceVisitor.visit(new Reconciliation("test", "kind", "namespace", "name"), k, new ResourceVisitor.Visitor() {
            @Override
            public <M extends AnnotatedElement & Member> void visitProperty(Reconciliation reconciliation, List<String> path, Object owner, M member, ResourceVisitor.Property<M> property, Object propertyValue) {

            }

            @Override
            public void visitObject(Reconciliation reconciliation, List<String> path, Object object) {

            }
        });
    }

    @Test
    public void testDoesNotThrowWithListenerList() {
        Kafka k = TestUtils.fromYaml("/example2.yaml", Kafka.class, true);
        assertThat(k, is(notNullValue()));
        ResourceVisitor.visit(new Reconciliation("test", "kind", "namespace", "name"), k, new ResourceVisitor.Visitor() {
            @Override
            public <M extends AnnotatedElement & Member> void visitProperty(Reconciliation reconciliation, List<String> path, Object owner, M member, ResourceVisitor.Property<M> property, Object propertyValue) {

            }

            @Override
            public void visitObject(Reconciliation reconciliation, List<String> path, Object object) {

            }
        });
    }
}
