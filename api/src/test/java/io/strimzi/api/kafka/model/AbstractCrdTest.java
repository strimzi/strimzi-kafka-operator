/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class AbstractCrdTest<R extends CustomResource> {

    private final Class<R> crdClass;
    private final String kind;

    protected AbstractCrdTest(Class<R> crdClass) {
        this.crdClass = crdClass;
        try {
            this.kind = crdClass.newInstance().getKind();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    protected void assertDesiredResource(R k, String resource) throws IOException {
        //assertNotNull("The resource " + resourceName + " does not exist", model);
        String content = TestUtils.readResource(getClass(), resource);
        if (content != null) {
            String ssStr = TestUtils.toYamlString(k);
            assertThat(content.trim(), is(ssStr.trim()));
        } else {
            fail("The resource " + resource + " does not exist");
        }
    }

    @Test
    public void roundTrip() throws IOException, ReflectiveOperationException {
        String resourceName = crdClass.getSimpleName() + ".yaml";
        R model = TestUtils.fromYaml(resourceName, crdClass);
        assertThat("The classpath resource \" + resourceName + \" does not exist", model, is(notNullValue()));
        ObjectMeta metadata = model.getMetadata();
        assertThat(metadata, is(notNullValue()));
        assertDesiredResource(model, crdClass.getSimpleName() + ".out.yaml");
        assertDesiredResource(TestUtils.fromYamlString(TestUtils.toYamlString(model), crdClass), crdClass.getSimpleName() + ".out.yaml");
    }

}
