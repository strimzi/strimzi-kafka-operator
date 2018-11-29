/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SecretGenerator {

    protected static final Logger log = LogManager.getLogger(SecretGenerator.class.getName());

    public static Secret generateSecret(ClusterCa clusterCa, Secret secret, String namespace, String secretName, String keyCertName, Labels labels, OwnerReference ownerReference) {
        Map<String, String> data = new HashMap<>();
        if (secret == null || clusterCa.certRenewed()) {
            log.debug("Generating certificates");
            try {
                log.debug(keyCertName + " certificate to generate");
                CertAndKey eoCertAndKey = clusterCa.generateSignedCert(secretName, Ca.IO_STRIMZI);
                data.put(keyCertName + ".key", eoCertAndKey.keyAsBase64String());
                data.put(keyCertName + ".crt", eoCertAndKey.certAsBase64String());
            } catch (IOException e) {
                log.warn("Error while generating certificates", e);
            }
            log.debug("End generating certificates");
        } else {
            data.put(keyCertName + ".key", secret.getData().get(keyCertName + ".key"));
            data.put(keyCertName + ".crt", secret.getData().get(keyCertName + ".crt"));
        }
        return createSecret(secretName, namespace, labels, ownerReference, data);
    }

    public static Secret createSecret(String name, String namespace, Labels labels, OwnerReference ownerReference, Map<String, String> data) {
        if (ownerReference == null) {
            return new SecretBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .endMetadata()
                    .withData(data).build();
        } else {
            return new SecretBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withOwnerReferences(ownerReference)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .endMetadata()
                    .withData(data).build();
        }
    }
}
