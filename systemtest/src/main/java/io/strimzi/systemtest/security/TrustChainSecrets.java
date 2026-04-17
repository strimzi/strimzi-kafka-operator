/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.skodjob.kubetest4j.security.CertAndKey;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;

import java.util.ArrayList;
import java.util.Collection;

import static io.strimzi.systemtest.security.SystemTestCertGenerator.exportToPemFiles;

/**
 * Fluent builder for creating Kubernetes certificate secrets. Instead of repeating the two-step
 * pattern of exporting certificates to PEM files and then creating Kubernetes Secrets, this builder
 * provides a single declarative point where a test case defines exactly which secrets to create.
 *
 * <p>When sourced from a {@link SystemTestCertBundle} via {@link #fromBundle}, convenience methods
 * for standard chain levels are available. When created via {@link #withoutBundle}, only
 * {@link #withCustom} can be used for arbitrary certificate combinations.</p>
 */
public final class TrustChainSecrets {

    public static final String TRUST_FULL_CHAIN = "trust-full-chain";
    public static final String TRUST_ROOT_INTERMEDIATE = "trust-root-intermediate";
    public static final String TRUST_ROOT_ONLY = "trust-root-only";
    public static final String TRUST_INTERMEDIATE_ONLY = "trust-intermediate-only";
    public static final String TRUST_LEAF_ONLY = "trust-leaf-only";
    public static final String TRUST_FOREIGN = "trust-foreign";
    public static final String TRUST_SUBLEAF = "trust-subleaf";

    private final SystemTestCertBundle bundle;
    private final Collection<CertEntry> entries = new ArrayList<>();

    private TrustChainSecrets(final SystemTestCertBundle bundle) {
        this.bundle = bundle;
    }

    /**
     * Creates a new builder without a certificate bundle.
     * Only {@link #withCustom} can be used (convenience methods require a bundle).
     *
     * @return a new TrustChainSecrets builder
     */
    public static TrustChainSecrets withoutBundle() {
        return new TrustChainSecrets(null);
    }

    /**
     * Creates a new builder sourced from the given certificate bundle.
     *
     * @param bundle the certificate bundle containing root, intermediate, and leaf CAs
     * @return a new TrustChainSecrets builder
     */
    public static TrustChainSecrets fromBundle(final SystemTestCertBundle bundle) {
        return new TrustChainSecrets(bundle);
    }

    /**
     * Adds a trust secret containing the full chain: Leaf CA + Intermediate CA + Root CA.
     *
     * @param secretName the name of the Kubernetes Secret to create
     * @return this builder
     */
    public TrustChainSecrets withFullChain(final String secretName) {
        return withCustom(secretName, bundle.getSystemTestCa(), bundle.getIntermediateCa(), bundle.getStrimziRootCa());
    }

    /**
     * Adds a trust secret containing: Intermediate CA + Root CA.
     *
     * @param secretName the name of the Kubernetes Secret to create
     * @return this builder
     */
    public TrustChainSecrets withRootAndIntermediate(final String secretName) {
        return withCustom(secretName, bundle.getIntermediateCa(), bundle.getStrimziRootCa());
    }

    /**
     * Adds a trust secret containing only the Root CA.
     *
     * @param secretName the name of the Kubernetes Secret to create
     * @return this builder
     */
    public TrustChainSecrets withRootOnly(final String secretName) {
        return withCustom(secretName, bundle.getStrimziRootCa());
    }

    /**
     * Adds a trust secret containing only the Intermediate CA.
     *
     * @param secretName the name of the Kubernetes Secret to create
     * @return this builder
     */
    public TrustChainSecrets withIntermediateOnly(final String secretName) {
        return withCustom(secretName, bundle.getIntermediateCa());
    }

    /**
     * Adds a trust secret containing only the Leaf CA (SystemTest CA).
     *
     * @param secretName the name of the Kubernetes Secret to create
     * @return this builder
     */
    public TrustChainSecrets withLeafOnly(final String secretName) {
        return withCustom(secretName, bundle.getSystemTestCa());
    }

    /**
     * Adds a trust secret with an arbitrary set of certificates.
     * Use this for non-standard chains such as foreign CAs or extended (subleaf) chains.
     *
     * @param secretName the name of the Kubernetes Secret to create
     * @param certs one or more certificates; the first certificate's private key is used
     * @return this builder
     */
    public TrustChainSecrets withCustom(final String secretName, final CertAndKey... certs) {
        entries.add(new CertEntry(secretName, certs));
        return this;
    }

    /**
     * Creates all queued trust secrets in Kubernetes.
     * For each entry, exports certificates to PEM files (if needed) and creates a Secret
     * with the default {@code "ca"} data prefix ({@code ca.crt}, {@code ca.key}).
     *
     * @param namespace the Kubernetes namespace
     * @param clusterName the Kafka cluster name (used for Secret labels)
     */
    public void build(final String namespace, final String clusterName) {
        for (CertEntry entry : entries) {
            SecretUtils.createCustomCertSecret(namespace, clusterName, entry.secretName(), exportToPemFiles(entry.certs()));
        }
    }

    private record CertEntry(String secretName, CertAndKey[] certs) { }

}
