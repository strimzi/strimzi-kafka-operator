/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator;

import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;

public class MockCertManager implements CertManager {

    private static final String CLUSTER_KEY = """
            -----BEGIN PRIVATE KEY-----
            MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDbFnJj90sKoM35
            VszJsfwNvO5dshoeFIb2idf7h+l0h3GMv29j+1XtmLJGzxiYy320KFZr3IKWbq+D
            abqdlqEqZm9NZ1Kq9d7mB10zulQce5JwVZ3FqpCmLku2jHCaDXzTKC3T/Xp0O9Oe
            8+42ysSMCTd8p8aZ4vAyJMCKcoyVCGHrUWVba40D7cQNOlhJplSzHZdLFYZ13kwz
            pT5GpDEPhGVmtF8qV918lSxvdpuepyeFdOSYY88FEMMLLrlZG4QCPyES4FpcUXMz
            zvZeLIlZnKNIYbao3Kx+yZv//wjC80/pqdyoZ5+K5hDxjby2+f+2dh0TadKRZC2p
            p+j3/z63AgMBAAECggEBAJN5iai25uGRmvSzNAi08VkCC2YwpBoJcUv1P9jGBST2
            oz2+AypHHfFgruixMPpxR/2EhZ/3gEPo3+ZSvlaj9XrIFzYATgpclR08acWPMF03
            5TwOtbQ/+zyRv09zO7zHRXYR/r9LSimBuBKwWnKxjRpCfgJAIZSmyU7HpH/NWcpa
            6khQen5zQVsjnlv3L5OSN8exP8okVYY01JmrKTyo3IWsOiTogr1ebe84xvG2QzkV
            fdL99poVfXXSBivLwgGPiCNkZFtwcjDRECzBHAcrwYT1AIHsvsWvdbrXilAh/388
            1v3HWw7lfnJi8WV9iAplq1YOEwqKN6z5Y/wwAZQafHECgYEA8F3lGtOUIMeaTXeI
            9xlXAM1MXQPPxguMFS9ilcnqlTdNJjJ18FBWM/Kn73RF9mTCQCYn+5o6MmoyMQIq
            rreBchP7Bzg7RCKVvPnippqwVDeLY7khdA0I1lH6mD7urhBvO5Fby3wKZeJfi2N9
            suQzEuRDEw5EDHQf/0rP4RBnrg8CgYEA6VZBUKuur885Q/knRUKdIG/mXYAcvmAV
            I6kjFCVRx7lQ8xLTiagKo0SKr0TM0Qf1+4vqTUZCOBtGlW/UBdGW8yPvXegyMHQg
            pMoNCTnxgXS77f4pnluQRQux8M8oWJnf11oGxDHhLw0T7kqBHl9K/dw/cItRS5Ui
            Dch/2YDMDNkCgYBNqZjXxRrsSHHTq9amOBrDWJHez9d3Hs4BHlFVImtYEQktWUp/
            /gUMPdAC72eXh9C3l1x9z8QT+/oBmbiewQ3jBQ+rsoB7sEz/RSH1QK/OVjAEZZGo
            hHmhfdVhEZxew1KdRYcKRSa66pyCVgAMJ+1UokoFwys7dt3Lx6lJB9roAwKBgCRN
            OxQl4aOQhcRBew6XcoKdZiWdzNsBb8iAg+iadcKw3hszDp4X+q+z9i+WcJcEugxM
            lEM5bwvzkmOlZkMRfH6PVKozebt4FawNk0GgNiaB1ssMA8WTUTqsux5P3GMMbXq/
            ktXrPLFpQ3SLOtNS2APuxB/qTNeJeCbUzq80DorhAoGBALAU7AWjjxgjCJlVKcfc
            iFeFh0W3HnNMQfn/ukha2Mg4Nl4GdLN18wx5VGpSCMEZ415cCy5S0SXxuUE5l6G1
            Pb2gYF9JyGhXJLMF2x9k4he4j/qHu9RCvixYlbb/jN59aifJxTeOG53g0Fo+H9U1
            432A3bs1MPT1Ew7zgnxjznt8
            -----END PRIVATE KEY-----
            """;
    private static final String CLUSTER_CERT = """
            -----BEGIN CERTIFICATE-----
            MIIDhjCCAm6gAwIBAgIJANzx2pPcYgmlMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNV
            BAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQg
            Q29tcGFueSBMdGQxEzARBgNVBAMMCmNsdXN0ZXItY2EwIBcNMTgwODIzMTYxOTU0
            WhgPMjExODA3MzAxNjE5NTRaMFcxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZh
            dWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMM
            CmNsdXN0ZXItY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDbFnJj
            90sKoM35VszJsfwNvO5dshoeFIb2idf7h+l0h3GMv29j+1XtmLJGzxiYy320KFZr
            3IKWbq+DabqdlqEqZm9NZ1Kq9d7mB10zulQce5JwVZ3FqpCmLku2jHCaDXzTKC3T
            /Xp0O9Oe8+42ysSMCTd8p8aZ4vAyJMCKcoyVCGHrUWVba40D7cQNOlhJplSzHZdL
            FYZ13kwzpT5GpDEPhGVmtF8qV918lSxvdpuepyeFdOSYY88FEMMLLrlZG4QCPyES
            4FpcUXMzzvZeLIlZnKNIYbao3Kx+yZv//wjC80/pqdyoZ5+K5hDxjby2+f+2dh0T
            adKRZC2pp+j3/z63AgMBAAGjUzBRMB0GA1UdDgQWBBThuvddCb/5TPSKYNOHkCTL
            VghhRzAfBgNVHSMEGDAWgBThuvddCb/5TPSKYNOHkCTLVghhRzAPBgNVHRMBAf8E
            BTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBA6oTI27dJgbVtyWxQWznKrkznZ9+t
            mQQGbpfl9zEg7/0X7fFb+m84QHro+aNnQ4kTgZ6QBvusIpwfx1F6lQrraVrPr142
            4DqGmY9xReNu/fj+C+8lTI5PA+mE7tMrLpQvKxI+AMttvlz8eo1SITUA+kJEiWZX
            mjvyHXmhic4K8SnnB0gnFzHN4y09wLqRMNCRH+aI+sa9Wu8cqvpTqlelVcYV83zu
            ydx4VZkC+zTzjI418znN/NU2CMpxLZNl0/zCrspID7v34NRmJ1AHFcrn7/XhsSvz
            D0z+vgrfionoRhyWUDh7POlWwdUOWiBDBOFrkgeKNphSC0glYFN+2IW7
            -----END CERTIFICATE-----
            """;
    private static final String CLIENTS_KEY = """
            -----BEGIN PRIVATE KEY-----
            MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCWws5FEJOpfZ/s
            FJWYbYdJVxmZB+5PjCwA2TUZxF/3P4w/5g2KZaXNy89AfBC5vRDRgyDyj/RwcDg8
            0kDGKobcGhTx5YkWoNvR/2WuTN6KC8DM78bfEREDHDxiXfAXrMIi7Ux2FvUX13l7
            6Sp9kiG3ETLjFom3n/qhg1ITJqPJSJi3tey0o2Pd5Arv0MIhQyep++URtZfND5fg
            F5x7hgnSf9Q1P1dJnVadu+ohUmmG7g+zX4rTqjN2jmHcf9V4lLKdPGWwLQEGnP9y
            Dqlm8x12M/BcIJasRgcciVsKYFuXe09NEYBvUjW8L6gaQ6U9wcYZ2MlKW/8LMGkS
            FfO4quAJAgMBAAECggEAQ1NdsEQV3UQHrfMHV1naZ6so+EktaILNh9d4OjiTLqRH
            aqW++EYqhDv3IvIEuh2vrBCmHwygebHzu12dpaGKNjLDlb8OuHc/k4k9jFgxrW5Q
            PHT719QUR9JNORSASuJQlC5qzfW0oGAOlYJsAkXHHqzkj7sZ51HfKE+v0HOaAyHj
            8gOeBNk1Mtb3Sj5mXpWFQGpXXuG01Vsjj7Nj/91a4KtWAWOqeagc2Bk+C0aZ7d1p
            SQcLVWjJYwoejgCc2elZxzbfmDtVSAgFtdTPxwf9uflMducTfp/RyaQbzuYSrSmz
            rnZq/59i9lYl314rjjkCusDaDSPdK5QziN54tQ+BcQKBgQDE9ulPecHtZhOsI9zT
            J+xTJtZq1w8kFV5jMqXnL3jAFBXsC3s02KLq36ppvf8kVzUHrHE+DiWnHKEIiy/U
            luMnPvJb/6qqdQNDpcrF+CE2JevvoPl5hrKdyzAI4TNu96aU+9qVrO2rB7bWBvlA
            dVwIZ8zkk3pwbdEj9rYpMA1VVQKBgQDD8rJAEd9fLtX53NQh8XWEJ1dEfncmg/ib
            0vyoYlqSDjPTot85sCunVZNHwUoKUsukzi+Tc9hxaXCjEB6ICVeXqWc4PYnbK79H
            N+2X6YaO/rKAzbxM1F/Km3IzzvoXFJnPG4hxvBmpdApKgBGOVixnjD7PzNz4jh9u
            1qhDocdf5QKBgQCDsLqporTgr0Ez9P5uR+Egb3UpFgVPkOH83R5Dhl/rvQIzQjHs
            UXQMKeNcs+XlPFF+gfNtFDRkmSWp+rXOI9xYnyOYE0belUHLdwwudQpvk8c9/pkO
            gdrm2bWSGlAzP22nawTo0ihOE+hRDXSVfmI8VHqP0XMpvKL6srd0rmYbyQKBgAYD
            PXr/0WXfTwuSviOogB2lA2WDp+5ToF5PtBcKpZLTwr1cwxLHGB/TXWiXQslcTwlo
            lkclB+A7BwzJ4tXzy29I8HTmVoOWLRFnYvAFZ26d3CZdqciFv8a8zF1QnZX1uN6F
            DsPGrNbpS6OLmH5QoJ4wzICd3a321noVNiaVIUQNAoGAYu4RrGcBKRuy75lfKARD
            gNxxVlvuI33ieK/3A9nUWc3LXl5D/yiSePCUs4giOwi2gFrGjcmIqLXZE5XUYGEu
            zXWWQCGbMqyX15/A2/eTuj658F292nkSyU/5U2999WjCm79sfnGJB1zavfv2fzGK
            g4trXCUkjAVG3Toaq05saGM=
            -----END PRIVATE KEY-----
            """;
    private static final String CLIENTS_CERT = """
            -----BEGIN CERTIFICATE-----
            MIIDhjCCAm6gAwIBAgIJAOKzFJgrn+rZMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNV
            BAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQg
            Q29tcGFueSBMdGQxEzARBgNVBAMMCmNsaWVudHMtY2EwIBcNMTgwODIzMTYyMTI1
            WhgPMjExODA3MzAxNjIxMjVaMFcxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZh
            dWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMM
            CmNsaWVudHMtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCWws5F
            EJOpfZ/sFJWYbYdJVxmZB+5PjCwA2TUZxF/3P4w/5g2KZaXNy89AfBC5vRDRgyDy
            j/RwcDg80kDGKobcGhTx5YkWoNvR/2WuTN6KC8DM78bfEREDHDxiXfAXrMIi7Ux2
            FvUX13l76Sp9kiG3ETLjFom3n/qhg1ITJqPJSJi3tey0o2Pd5Arv0MIhQyep++UR
            tZfND5fgF5x7hgnSf9Q1P1dJnVadu+ohUmmG7g+zX4rTqjN2jmHcf9V4lLKdPGWw
            LQEGnP9yDqlm8x12M/BcIJasRgcciVsKYFuXe09NEYBvUjW8L6gaQ6U9wcYZ2MlK
            W/8LMGkSFfO4quAJAgMBAAGjUzBRMB0GA1UdDgQWBBQUwNmfsNj+PM240pVPxYx9
            Q9eQhDAfBgNVHSMEGDAWgBQUwNmfsNj+PM240pVPxYx9Q9eQhDAPBgNVHRMBAf8E
            BTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAnhbNKwMmnayHsT6kKgyyDV6RUUYs6
            nYf3nx+GIQWSw4c5TOHDcTWdKpOxVnLNXYKQoSkb1RBoSMLBdQwidZ5K2DB5eXaG
            rcfEbKNBc5ZCFgFEAyy35pitJOmU/KzCdKyvx+TR5hIgGoKajYX5JZxj+1rTPGKO
            ePT9iFp1ZbzHjgw6vFeJ+D2ov6HfW6C/KuK9Y6xUpvRQLVjMJYCyzxkxQAxZvu/0
            0HVYYH6UJ7kuWywFMWoBdZ8US/vuUSBYyCGNL9p6ol+h9rsz3cIWBVBjx8C3qKki
            QtlIdmFljGSaGGY6aJjUvUdgoPp1yQPa5oS+afr5g9gaEp4lxP6mc+Li
            -----END CERTIFICATE-----
            """;

    private static final String CERT_STORE_PASSWORD = "123456";

    private static final byte[] CLUSTER_CERT_STORE;
    private static final byte[] CLIENTS_CERT_STORE;

    /**
     * End entity cert is used for server and user certificates
     * This is a certificate signed by the cluster-ca certificate,
     * however it can safely be used as a user certificate as long as the tests
     * don't verify the signing authority.
     *
     * Certificate:
     * ============
     * Issuer: C = XX, L = Default City, O = Default Company Ltd, CN = cluster-ca
     * Validity
     *      Not Before: Jan 12 11:55:12 2023 GMT
     *      Not After : Dec 19 11:55:12 2122 GMT
     * Subject: C = DE, ST = NRW, L = Dusseldorf, O = Strimzi, OU = Strimzi, CN = StrimziTest
     */
    private static final String END_ENTITY_CERT = """
            -----BEGIN CERTIFICATE-----
            MIIESjCCAzICFAom42BJSu8N8fTix+wYFeTnFXkrMA0GCSqGSIb3DQEBCwUAMFcx
            CzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0Rl
            ZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMMCmNsdXN0ZXItY2EwIBcNMjMwMTEy
            MTE1NTEyWhgPMjEyMjEyMTkxMTU1MTJaMGoxCzAJBgNVBAYTAkRFMQwwCgYDVQQI
            DANOUlcxEzARBgNVBAcMCkR1c3NlbGRvcmYxEDAOBgNVBAoMB1N0cmltemkxEDAO
            BgNVBAsMB1N0cmltemkxFDASBgNVBAMMC1N0cmltemlUZXN0MIICIjANBgkqhkiG
            9w0BAQEFAAOCAg8AMIICCgKCAgEAtLNAOsVmLa05JnCq6n8aUf5IqqWjR0Lw4Gn2
            gqxIbe7+q88jmqPAF/aUIBmUCOcmS+GC+EaAi522mociYUcveonfoOUaSh8T23a3
            LY4ACq2NZsUH96qUTSieFFELVSz/XXwOuAI+HCZTvG/veRFCPzL2HAJdqmTJyh9v
            tknedKdQvhhyQYpERPtFv0DhzMw2/PUgDs3Zf1OWByv2X3xGBydCHL9ahoJzaArZ
            A+jIXTAQl1y4T0uFusLxWjiAQ96xJnA/6d+9Rt0Urxj7H9RpChVw1vJ+qekEvrNT
            1HrVsE26LkQzE8kL5tHAYUeFs+KIN5VKxq+cN5DLJ6eZ6Us7N0hGZrd9gg8ntlSb
            /gsCA+SNPyVB+ix9NKvhlduPKS6N/EBsJBbiVeRzGTDuiNTHgjv/58/bdZGZ3R+9
            KUMB4xUcO2CMvQWgd1ZDoi4PM91PERpqAj4UDFU07F3k0K8nunjk3OW3Q75eE+6L
            3q6+Gssz8aGLzP1muZZTtZ/ZCeO46FmQ42Yt8uMS8/UMHleRAENTY9sEyFrfN3As
            zXrXK4SdP/9dRgqjm8JalcqwkiJDR8+LTpYSy+uTY7dD3C2H1mn0eqLZp3RamvpS
            zQcIvjQPGZWpLvvsy7V8P0jW9yA7+l5c/bqzy6B/IUAfayce7kBdICIZe9T1F4JP
            eOfB03kCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEATtZnkLygVsQX39oS+ra/eI0U
            w1Nr03RSn/CYPEe0B9UI7FxprcbzBnQyVhzgUcr7e6aOKfAM92k1uCcFwg1YOZcB
            u2YNGDum7AjwyazRKCXX30QY3eKyBuWCsA96z2Z8gxvmqtxKmk7X0LsoNQ4qWvxV
            yPlKo9idFuhf2IhFwd1ucX3S+ZWRXxzQGeLjdpqf0vBUA9uqP5bjMZHwums1s/MB
            MyngL6wecFymyjqK3kduVNKvIo/juPq0NV8u70gdHltsaJArw4sfMw/4LnR++hRD
            EgqE8p9hEapdshRN9+YQchN44URq4xwcE/fDebru+IxPbkDOCMsj5x1WcUCg7w==
            -----END CERTIFICATE-----""";

    // End entity key is used for server and user certificates
    private static final String END_ENTITY_KEY = """
            -----BEGIN PRIVATE KEY-----
            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
            UzKvfqO8LHHn+gkP6Z7GXxmD1/Eu
            -----END PRIVATE KEY-----""";


    // End entity key store is used for server and user certificates.
    // We don't need a real store here.
    private static final String END_ENTITY_KEY_STORE = "key store";


    static {
        InputStream is = MockCertManager.class.getClassLoader().getResourceAsStream("CLUSTER_CERT.str");
        CLUSTER_CERT_STORE = loadResource(is);
        is = MockCertManager.class.getClassLoader().getResourceAsStream("CLIENTS_CERT.str");
        CLIENTS_CERT_STORE = loadResource(is);
    }

    public static String clusterCaCert() {
        return Base64.getEncoder().encodeToString(CLUSTER_CERT.getBytes(Charset.defaultCharset()));
    }

    public static String clusterCaKey() {
        return Base64.getEncoder().encodeToString(CLUSTER_KEY.getBytes(Charset.defaultCharset()));
    }

    public static String clientsCaCert() {
        return Base64.getEncoder().encodeToString(CLIENTS_CERT.getBytes(Charset.defaultCharset()));
    }

    public static String clientsCaKey() {
        return Base64.getEncoder().encodeToString(CLIENTS_KEY.getBytes(Charset.defaultCharset()));
    }

    public static String clusterCaCertStore() {
        return Base64.getEncoder().encodeToString(CLUSTER_CERT_STORE);
    }

    public static String clientsCaCertStore() {
        return Base64.getEncoder().encodeToString(CLIENTS_CERT_STORE);
    }

    public static String certStorePassword() {
        return Base64.getEncoder().encodeToString(CERT_STORE_PASSWORD.getBytes(Charset.defaultCharset()));
    }

    private void write(File keyFile, String str) throws IOException {
        try (FileWriter writer = new FileWriter(keyFile)) {
            writer.write(str);
        }
    }

    private static byte[] loadResource(InputStream is) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            int read;
            byte[] data = new byte[2048];
            while ((read = is.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, read);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer.toByteArray();
    }

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile  path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param sbj      subject information
     * @param days     certificate duration
     * @throws IOException
     */
    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {

        write(keyFile, CLUSTER_KEY);
        write(certFile, CLUSTER_CERT);
    }

    /**
     * Renew a new self-signed certificate, keeping the existing private key
     *
     * @param keyFile  path to the file containing the existing private key
     * @param certFile path to the file which will contain the new self signed certificate
     * @param sbj      subject information
     * @param days     certificate duration
     * @throws IOException
     */
    @Override
    public void renewSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {
        generateSelfSignedCert(keyFile, certFile, sbj, days);
    }

    @Override
    public void generateRootCaCert(Subject subject, File subjectKeyFile, File subjectCertFile, ZonedDateTime notBefore, ZonedDateTime notAfter, int pathLength) throws IOException {
        generateSelfSignedCert(subjectKeyFile, subjectCertFile, subject, 1);
    }

    @Override
    public void generateIntermediateCaCert(File issuerCaKeyFile, File issuerCaCertFile, Subject subject, File subjectKeyFile, File subjectCertFile, ZonedDateTime notBefore, ZonedDateTime notAfter, int pathLength) throws IOException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void addCertToTrustStore(File certFile, String certAlias, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        if (certFile.getName().contains("cluster")) {
            Files.write(trustStoreFile.toPath(), CLUSTER_CERT_STORE);
        } else if (certFile.getName().contains("clients")) {
            Files.write(trustStoreFile.toPath(), CLIENTS_CERT_STORE);
        }
    }

    @Override
    public void addKeyAndCertToKeyStore(File keyFile, File certFile, String alias, File keyStoreFile, String keyStorePassword) throws IOException {
        write(keyStoreFile, END_ENTITY_KEY_STORE);
    }

    @Override
    public void deleteFromTrustStore(List<String> aliases, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        // never called during the tests which use this MockCertManager
        throw new RuntimeException("Not implemented");
    }

    /**
     * Generate a certificate sign request
     *
     * @param keyFile path to the file which will contain the private key
     * @param csrFile path to the file which will contain the certificate sign request
     * @param sbj     subject information
     */
    @Override
    public void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException {
        write(keyFile, END_ENTITY_KEY);
        // A real CSR file is not needed for tests to pass
        write(csrFile, "csr file");
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, Subject sbj, int days) throws IOException {
        write(crtFile, END_ENTITY_CERT);
    }

    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, Subject sbj, int days) throws IOException {
        write(crtFile, END_ENTITY_CERT);
    }

    public static String serverCert() {
        return END_ENTITY_CERT;
    }

    public static String serverKey() {
        return END_ENTITY_KEY;
    }

    public static String serverKeyStore() {
        return END_ENTITY_KEY_STORE;
    }

    public static String userCert() {
        return END_ENTITY_CERT;
    }

    public static String userKey() {
        return END_ENTITY_KEY;
    }

    public static String userKeyStore() {
        return END_ENTITY_KEY_STORE;
    }
}
