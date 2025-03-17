/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.kubernetes;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateCondition;
import io.fabric8.certmanager.api.model.v1.CertificateList;
import io.fabric8.certmanager.api.model.v1.CertificateStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Operations for CertManager resources such as {@code Certificate}.
 */
public class CertManagerCertificateOperator extends AbstractNamespacedResourceOperator<KubernetesClient, Certificate, CertificateList, Resource<Certificate>> {
    /**
     * Constructor
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client
     */
    public CertManagerCertificateOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "Certificate");
    }

    @Override
    protected MixedOperation<Certificate, CertificateList, Resource<Certificate>> operation() {
        return client.certificates().v1().resources(Certificate.class, CertificateList.class);
    }

    /**
     * Wait for provided Certificate object to report ready
     * @param reconciliation Reconciliation marker
     * @param namespace Namespace of the Certificate for
     * @param name Name of the Certificate to wait for
     * @return Future that completes when the Certificate is ready
     */
    public CompletionStage<Void> waitForReady(Reconciliation reconciliation, String namespace, String name) {
        return waitFor(reconciliation, namespace, name, 1_000, 300000, this::isReady);
    }

    private boolean isReady(String namespace, String name) {
        Certificate certificate = operation().inNamespace(namespace).withName(name).get();
        CertificateStatus status = certificate.getStatus();
        boolean certificateReady = false;
        if (status != null) {
            List<CertificateCondition> conditions = certificate.getStatus().getConditions();
            Optional<CertificateCondition> readyCondition = conditions.stream().filter(condition -> condition.getType().equals("Ready"))
                    .findFirst();
            if (readyCondition.isPresent() && readyCondition.get().getStatus().equals("True")) {
                certificateReady = true;
            }
        }
        return certificateReady;
    }
}
