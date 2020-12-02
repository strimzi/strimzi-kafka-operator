/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import java.util.ArrayList;
import java.util.List;

public class MultipartConversions {
    private static final ThreadLocal<MultipartConversions> TL = ThreadLocal.withInitial(MultipartConversions::new);

    private List<MultipartResource> resources = new ArrayList<>();

    private MultipartConversions() {
    }

    public static MultipartConversions get() {
        return TL.get();
    }

    public static void remove() {
        TL.remove();
    }

    public List<MultipartResource> getResources() {
        return resources;
    }

    public void addFirst(MultipartResource resource) {
        resources.add(0, resource);
    }

    public void addLast(MultipartResource resource) {
        resources.add(resource);
    }
}

