/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.test.TestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class AbstractCrdTest<R extends CustomResource, T extends VisitableBuilder<R, T>> {

    private final Class<R> crdClass;
    private final String kind;
    private final Class<T> visitorClass;

    protected AbstractCrdTest(Class<R> crdClass, Class<T> visitorClass) {
        this.crdClass = crdClass;
        this.visitorClass = visitorClass;
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
            assertEquals(content.trim(), ssStr.trim());
        } else {
            fail("The resource " + resource + " does not exist");
        }
    }

    @Test
    public void roundTrip() throws IOException {
        String resourceName = crdClass.getSimpleName() + ".yaml";
        R model = TestUtils.fromYaml(resourceName, crdClass);
        assertNotNull("The classpath resource " + resourceName + " does not exist", model);
        ObjectMeta metadata = model.getMetadata();
        assertNotNull(metadata);
        assertDesiredResource(model, crdClass.getSimpleName() + ".out.yaml");
        assertDesiredResource(TestUtils.fromYamlString(TestUtils.toYamlString(model), crdClass), crdClass.getSimpleName() + ".out.yaml");
    }

}
