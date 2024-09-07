/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.test.ReadWriteUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public abstract class AbstractCrdTest<R extends CustomResource> {

    private final Class<R> crdClass;

    protected AbstractCrdTest(Class<R> crdClass) {
        this.crdClass = crdClass;
        assertDoesNotThrow(() -> crdClass.getDeclaredConstructor().newInstance().getKind());
    }

    protected void assertDesiredResource(R actual, String expectedResource) {
        String content = ReadWriteUtils.readFileFromResources(getClass(), expectedResource);
        assertThat("The resource " + expectedResource + " does not exist", content, is(notNullValue()));

        String ssStr = ReadWriteUtils.writeObjectToYamlString(actual);
        assertThat(ssStr.trim(), is(content.trim()));
    }

    @Test
    public void roundTrip() {
        String resourceName = crdClass.getSimpleName() + ".yaml";
        R model = ReadWriteUtils.readObjectFromYamlFileInResources(resourceName, crdClass);
        assertThat("The classpath resource " + resourceName + " does not exist", model, is(notNullValue()));

        ObjectMeta metadata = model.getMetadata();
        assertThat(metadata, is(notNullValue()));
        assertDesiredResource(model, crdClass.getSimpleName() + ".out.yaml");
        assertDesiredResource(ReadWriteUtils.readObjectFromYamlString(ReadWriteUtils.writeObjectToYamlString(model), crdClass), crdClass.getSimpleName() + ".out.yaml");
    }

}
