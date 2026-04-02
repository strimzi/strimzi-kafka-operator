/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.crdgenerator.annotations.ExternalLink;
import io.strimzi.crdgenerator.annotations.KubeLink;

/**
 * Class for handling links in the documentation generator
 */
public class Linker {
    private final String baseUrl;

    /**
     * Constructs the Linker and initializes the base URL to the Kubernetes documentation
     *
     * @param baseUrl   The base URL to the Kubernetes documentation which should be used
     */
    public Linker(String baseUrl) {
        // E.g. https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/
        this.baseUrl = baseUrl;
    }

    /**
     * Creates the link based on the annotations on the property
     *
     * @param property  Property to check the annotation on
     *
     * @return  Returns the URL based on the annotation or null if no link annotation is present
     */
    public String link(Property property)    {
        KubeLink kubeLink = property.getAnnotation(KubeLink.class);
        ExternalLink externalLink = property.getAnnotation(ExternalLink.class);

        if (kubeLink != null) {
            return link(kubeLink);
        } else if (externalLink != null) {
            return link(externalLink);
        } else {
            return null;
        }
    }

    /**
     * Generates URL to a specific page in the Kubernetes API reference.
     *
     * @param kubeLink  The KubeLink annotation which specifies the Kubernetes API to link to
     *
     * @return  An HTTP link deep-linking to the Kubernetes documentation
     */
    private String link(KubeLink kubeLink)   {
        // E.g. https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#networkpolicyingressrule-v1-networking-k8s-io
        return baseUrl + "#" + kubeLink.kind() + "-" + kubeLink.version() + "-" + kubeLink.group().replace(".", "-");
    }

    /**
     * Returns URL to a specific page in the API reference documentation of some other project.
     *
     * @param externalLink  The ExternalLink annotation which specifies the external URL
     *
     * @return  An HTTP URL
     */
    private String link(ExternalLink externalLink)   {
        return externalLink.url();
    }
}
