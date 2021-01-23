/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import java.io.File;

public class CertAndKeyFiles {

    private final File certFile;
    private final File keyFile;

    public CertAndKeyFiles(File certFile, File keyFile) {
        this.certFile = certFile;
        this.keyFile = keyFile;
    }

    public String getCertPath() {
        return certFile.getPath();
    }

    public String getKeyPath() {
        return keyFile.getPath();
    }
}
