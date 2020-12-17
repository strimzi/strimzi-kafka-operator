/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ServiceMockBuilder extends MockBuilder<Service, ServiceList, ServiceResource<Service>> {

    private static final Logger LOGGER = LogManager.getLogger(ServiceMockBuilder.class);

    private final Map<String, Endpoints> endpointsDb;

    public ServiceMockBuilder(Map<String, Service> svcDb, Map<String, Endpoints> endpointsDb) {
        super(Service.class, ServiceList.class, castClass(ServiceResource.class), svcDb);
        this.endpointsDb = endpointsDb;
    }

    /** Override Service creation to also create Endpoints */
    @Override
    protected void mockCreate(String resourceName, ServiceResource<Service> resource) {
        when(resource.create(any(Service.class))).thenAnswer(i -> {
            Service argument = i.getArgument(0);
            db.put(resourceName, copyResource(argument));
            LOGGER.debug("create {} (and endpoint) {} ", resourceType, resourceName);
            endpointsDb.put(resourceName, new Endpoints());
            return argument;
        });
    }
}
