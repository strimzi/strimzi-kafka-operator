/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import java.util.ArrayList;
import java.util.List;

/**
 * A container for the conversions which result in multiple resources
 */
public class MultipartConversions {
    private static final ThreadLocal<MultipartConversions> TL = ThreadLocal.withInitial(MultipartConversions::new);

    private final List<MultipartResource> resources = new ArrayList<>();

    private MultipartConversions() { }

    /**
     * Gets the MultipartConversions instance for the current thread.
     *
     * @return  Get the MultipartConversions instance for the current thread
     */
    public static MultipartConversions get() {
        return TL.get();
    }

    /**
     * Remove the MultipartConversions instance for the current thread
     */
    public static void remove() {
        TL.remove();
    }

    /**
     * Gets the list of resources created during conversion.
     *
     * @return  The list of resources which were created during the conversion
     */
    public List<MultipartResource> getResources() {
        return resources;
    }

    /**
     * Add a resource to the first place in the list of resources which were created during the conversion
     *
     * @param resource  Resource to be added
     */
    @SuppressWarnings("unused") // Might be useful one day
    public void addFirst(MultipartResource resource) {
        resources.add(0, resource);
    }

    /**
     * Add a resource to the last place in the list of resources which were created during the conversion
     *
     * @param resource  Resource to be added
     */
    public void addLast(MultipartResource resource) {
        resources.add(resource);
    }
}

