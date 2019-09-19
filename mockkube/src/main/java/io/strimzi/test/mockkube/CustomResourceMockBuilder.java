/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

class CustomResourceMockBuilder<T extends CustomResource, L extends KubernetesResource & KubernetesResourceList<T>, D extends Doneable<T>, S>
        extends MockBuilder<T, L, D, Resource<T, D>> {

    private static final Logger LOGGER = LogManager.getLogger(CustomResourceMockBuilder.class);

    private final MockKube.MockedCrd<T, L, D, S> mockedCrd;

    public CustomResourceMockBuilder(MockKube.MockedCrd<T, L, D, S> mockedCrd) {
        super(mockedCrd.getCrClass(), mockedCrd.getCrListClass(), mockedCrd.getCrDoneableClass(), castClass(Resource.class), mockedCrd.getInstances());
        this.mockedCrd = mockedCrd;
    }

    @Override
    public void updateStatus(String namespace, String name, T resource) {
        checkDoesExist(name);
        Function<T, S> getStatus = mockedCrd.getStatus();
        if (getStatus != null) {
            S status = getStatus.apply(copyResource(resource));
            LOGGER.debug("Updating status on {} to {}", resourceTypeClass.getSimpleName(), status);
            T t = incrementResourceVersion(copyResource(db.get(name)));
            getStatus.apply(t);
            mockedCrd.setStatus().accept(t, status);
            db.put(name, t);
            fireWatchers(name, t, Watcher.Action.MODIFIED, "updateStatus");
        }
    }
}
