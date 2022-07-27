/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

/**
 * This package provides interfaces for the pluggable PodSecurityProviders which are used to configure the Pod and
 * container security profiles. Strimzi provides two built-in providers for the baseline and restricted Kubernetes
 * security profiles. In addition, users can provide their own implementations by implementing the PodSecurityProvider
 * interface, packaging it into a JAR and adding the JAR to the Strimzi Cluster Operator image.
 *
 * The pluggable PodSecurityProvider mechanism was introduced in Strimzi 0.31 based on the
 * proposal https://github.com/strimzi/proposals/blob/main/037-pluggable-pod-security-profiles.md
 *
 * @since 0.31.0
 */
package io.strimzi.plugin.security.profiles;