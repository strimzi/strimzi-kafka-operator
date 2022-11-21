/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.crdgenerator.annotations.KubeLink;

/**
 * Interface for handling links in the documentation generator
 */
interface Linker {
    /**
     * Generates URL to some specific documentation
     *
     * @param kubeLink  Specifies the Kubernetes API which should the link point to
     *
     * @return  An HTTP link deep-linking to the Kubernetes documentation
     */
    String link(KubeLink kubeLink);
}
