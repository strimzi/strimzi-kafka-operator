/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Util;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PemTrustSetTest {
    public static final String NAMESPACE = "testns";
    public static final String CLUSTER = "testcluster";

    @Test
    public void testSecretCorrupted() {
        Secret secretWithBadCertificate = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName(CLUSTER))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", "notacert"))
                .build();
        PemTrustSet pemTrustSet = new PemTrustSet(secretWithBadCertificate);
        Exception e = assertThrows(RuntimeException.class, pemTrustSet::jksTrustStore);
        assertThat(e.getMessage(), is("Bad/corrupt certificate found in data.ca.crt of Secret testcluster-cluster-operator-certs in namespace testns"));
    }

    @Test
    public void testMissingSecret() {
        Exception e = assertThrows(NullPointerException.class, () -> new PemTrustSet(null));
        assertThat(e.getMessage(), is("Cannot extract trust set from null secret."));
    }

    @Test
    public void testEmptySecretData() {
        Secret secretWithEmptyData = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName(CLUSTER))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Map.of())
                .build();
        Exception e = assertThrows(RuntimeException.class,  () -> new PemTrustSet(secretWithEmptyData));
        assertThat(e.getMessage(), is("The Secret testns/testcluster-cluster-operator-certs does not contain any fields with the suffix .crt"));
    }

    @Test
    public void testNoCertsInSecretData() {
        Secret secretWithNoCerts = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName(CLUSTER))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.txt", "not-a-cert"))
                .build();
        Exception e = assertThrows(RuntimeException.class, () -> new PemTrustSet(secretWithNoCerts));
        assertThat(e.getMessage(), is("The Secret testns/testcluster-cluster-operator-certs does not contain any fields with the suffix .crt"));
    }

    @Test
    public void testCAChain() throws GeneralSecurityException, IOException {
        // This certificate is used for testing purposes only and is not a real certificate. It is valid until 2126,
        // so it should not cause any issues with the tests.
        String caChain = """
                -----BEGIN CERTIFICATE-----
                MIIDjDCCAnSgAwIBAgIUOFLIiKVSY/n2xHQzRUG6kMBhEaMwDQYJKoZIhvcNAQEL
                BQAwTzELMAkGA1UEBhMCQ1oxDzANBgNVBAcTBlByYWd1ZTEWMBQGA1UEChMNU3Ry
                aW16aSwgSW5jLjEXMBUGA1UEAxMOSW50ZXJtZWRpYXRlQ0EwIBcNMjYwMjEzMjA1
                MjAwWhgPMjEyNjAxMjAyMDUyMDBaMEoxCzAJBgNVBAYTAkNaMQ8wDQYDVQQHEwZQ
                cmFndWUxFjAUBgNVBAoTDVN0cmltemksIEluYy4xEjAQBgNVBAMTCUNsdXN0ZXJD
                QTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKbe9/1uQWfHNKRKMUxX
                xTu13mu80CDcLv7ydTgg44Kzf7TnGWrmESuISceyZyfUCzXsGItByzrb78hz6tj6
                dv/8N6kpj1yTAj0DqwdTCRhbyUvK/C+0rgi5YkBIMN+ZdihEA99kfo2mJEZatLFr
                r//MC0SS5DclrkujaxL9oeuET2764oFNvsoz43dS4m/iIlEEXgvIa/wugluJNfbb
                FPAxL+YoMCuf2ybfKKy8Eo5dPh14AOtfL5uyVE70JXDK1LqktBtdHQRETOvNZPEI
                bj9FUZM4vVD34vXXWwLjTnx1o1PttS8mj8p5TMtpqXT4toLaxFevgOiSoqAIbtMJ
                cbUCAwEAAaNjMGEwDgYDVR0PAQH/BAQDAgIEMA8GA1UdEwEB/wQFMAMBAf8wHQYD
                VR0OBBYEFHdcdKdarpL368/s4jGpRhRHpEvDMB8GA1UdIwQYMBaAFA9Fok9aepFa
                HaAO4xlJjp29rmcSMA0GCSqGSIb3DQEBCwUAA4IBAQB3WS4IP4WPgq/y3PHzAhq6
                OpTx17DjxscZZ8gox9PDGRXK5IkkDEa+waRTq1DaEwL7XV+nHWSo77QxsIuk/m4h
                sVKUtJ5muJ/x23iHKesq4Kr+GWMKdOd9LSftQEkF7L97sLl+51cwcrCbYN47cHt7
                bfZwJ2C+hqEpuLGtGREXzsGwe27ta8OC8gfJV2cMd9QtVj7QOCFvzPTpIRqEFVb2
                mV3XZnvI+FtNtuP86wNLYP1YDFIhEt0G+KBY4BIgI1VjOFJKAVpDCBoNO2+TUE0a
                UJ7ftdxyKWnytI2fezgtAFEFJzE0HMLXPORDU4cxOfXfFQRteFRAl/Tzju/iQbTv
                -----END CERTIFICATE-----
                -----BEGIN CERTIFICATE-----
                MIIDiTCCAnGgAwIBAgIUT862ZuG8dKtH+CPiR235prmMO20wDQYJKoZIhvcNAQEL
                BQAwRzELMAkGA1UEBhMCQ1oxDzANBgNVBAcTBlByYWd1ZTEWMBQGA1UEChMNU3Ry
                aW16aSwgSW5jLjEPMA0GA1UEAxMGUm9vdENBMCAXDTI2MDIxMzIwNTIwMFoYDzIx
                MjYwMTIwMjA1MjAwWjBPMQswCQYDVQQGEwJDWjEPMA0GA1UEBxMGUHJhZ3VlMRYw
                FAYDVQQKEw1TdHJpbXppLCBJbmMuMRcwFQYDVQQDEw5JbnRlcm1lZGlhdGVDQTCC
                ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAN4EY/bi0XxaCn5dGBTpEAXS
                svGgzUi7Xk+canttWiRC+TyPvrBExYYHzfEZBZLYp9RzrfXK544Yn0MXoiJQTkfO
                cVxafVJcPyFN9yU1uALg05CU+SR+apnFWJr3Ii1KYnwqBqjkG7LrnhNdVh2n740o
                FCwKenfTbh4NOUnxqGLcNTh3I9W+Ivw+e6uVPJF6Hx0/Rss5Jt7Us/7jOg76zSg1
                5f6MomAqEQ6SdZBRkpjWBKzbVOR0qLxOPs6ExwOjUdZQR56QdtCaLVBrdZBxviUM
                UroAMB0cb1v3JrDZ3Sf8tsJLHM0nFCY5yOGibEtS3NwRYe43Lu3PZTcKEYgwebcC
                AwEAAaNjMGEwDgYDVR0PAQH/BAQDAgIEMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0O
                BBYEFA9Fok9aepFaHaAO4xlJjp29rmcSMB8GA1UdIwQYMBaAFJdwhhp3vopTk5wB
                6g8NJ20VF2fJMA0GCSqGSIb3DQEBCwUAA4IBAQBr1Q+9fveCnXr2W27bwOt/DCCO
                khgaxD8wRzIPfgFBC2zU4Ux3VqZKSQSRQu6Hf6pFYJC6GpOPF5KWAD4Dc8izYrmq
                hFKJjt7D4ZVbqhn8P05RtlRGSeGXSwkaP3YpXBrbpyAp4ctS4y3yusWnHmduaeX1
                OFyLUMCdc3sjw0l07r9yHfT1HoGwPXp9tt2hjZnbWL5pGobwNQF1bliH8K0XA9Sh
                mEG6jyNcgXNloXYf6DW6tlVDfJLsWLe3XvHZvWoELRtbOiLdwxA17XFiuNkyuhbX
                b5lR6GvcEyIoXoK9nOfFPWdlK0xi2sriqs1xedTX8xxeCe/yLx7OMdLtNAa8
                -----END CERTIFICATE-----
                -----BEGIN CERTIFICATE-----
                MIIDXjCCAkagAwIBAgIUc2Y/2d0Q02CEJg9uJMqFHzfGdM8wDQYJKoZIhvcNAQEL
                BQAwRzELMAkGA1UEBhMCQ1oxDzANBgNVBAcTBlByYWd1ZTEWMBQGA1UEChMNU3Ry
                aW16aSwgSW5jLjEPMA0GA1UEAxMGUm9vdENBMB4XDTI2MDIxMzIwNTIwMFoXDTMx
                MDIxMjIwNTIwMFowRzELMAkGA1UEBhMCQ1oxDzANBgNVBAcTBlByYWd1ZTEWMBQG
                A1UEChMNU3RyaW16aSwgSW5jLjEPMA0GA1UEAxMGUm9vdENBMIIBIjANBgkqhkiG
                9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmkk4mgquavBrubOiptbUJU54gP0FQIIIygW3
                HMMbi3fJquSd/Tt4lL4WVxOuVZdTHsviH6ZMiYeFcGdkFd/WBnBAxSplrFHPRI3H
                dFmjWg63uTY/BIqKtcx8ISsR6Y33x3Zq0oKHbPpiAZpUQAdt5YiofVhlI+aBwUay
                I2r7UJFvhV9wSAbX4ee4P7tqX/PFqUi20ef/8R7GG5lC+sHVTHuppWd0Xrz/4k0K
                mwImzA0pMfj9MwjlDoWeS7WpmFhqH1Q41YQ8bsY9zG6XzE3QdJd+42lVrn710I9t
                LF1YnwT4fw/WCMnodqLVUI6dOtyp9LQ64cu6Z+pZjrGErz6WQwIDAQABo0IwQDAO
                BgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUl3CGGne+
                ilOTnAHqDw0nbRUXZ8kwDQYJKoZIhvcNAQELBQADggEBADL/2AJ3ryKetM5bYLBL
                vywRsd5lLdmsXtjIYkBeOrs2Gjsql2GVRF00BnFHtCoqqjbgpZBgawcE1MmOfcCk
                eu1ds+0W1Ktoo13nEUMpDZakm1hluoZFZQQEW/9Nbr/cDzl+HqTxPCiefN6Y36xJ
                Dd6jM3KB4mkcSeBOMgE5ZbBdVqmjQa8LC3nZ7Y+UYBDqAVnGtAWQuFqyhWzMTvOv
                3youpnifUkMERjgjdrw8/A7LFe7M3KTsUmQ6IuTot3AJVAuBNnGagp9JWTQqsZE9
                t11Y2OQo9gVGyvyTRz+6UIpPuBNV5e+72Om5O5HlAeioyWXqZqaXGUXwtl12xyzI
                PjE=
                -----END CERTIFICATE-----""";

        Secret secretWithCAChain = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName(CLUSTER))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", Util.encodeToBase64(caChain)))
                .build();

        PemTrustSet pemTrustSet = new PemTrustSet(secretWithCAChain);

        // Test JKS conversion
        KeyStore jks = pemTrustSet.jksTrustStore();
        assertThat(jks.size(), is(1));
        X509Certificate cert = (X509Certificate) jks.getCertificate(jks.aliases().nextElement());
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=ClusterCA,O=Strimzi\\, Inc.,L=Prague,C=CZ"));

        // Test PEM conversion
        String pem = pemTrustSet.trustedCertificatesString();
        assertThat(pem, is("-----BEGIN CERTIFICATE-----\n" +
                "MIIDjDCCAnSgAwIBAgIUOFLIiKVSY/n2xHQzRUG6kMBhEaMwDQYJKoZIhvcNAQEL\n" +
                "BQAwTzELMAkGA1UEBhMCQ1oxDzANBgNVBAcTBlByYWd1ZTEWMBQGA1UEChMNU3Ry\n" +
                "aW16aSwgSW5jLjEXMBUGA1UEAxMOSW50ZXJtZWRpYXRlQ0EwIBcNMjYwMjEzMjA1\n" +
                "MjAwWhgPMjEyNjAxMjAyMDUyMDBaMEoxCzAJBgNVBAYTAkNaMQ8wDQYDVQQHEwZQ\n" +
                "cmFndWUxFjAUBgNVBAoTDVN0cmltemksIEluYy4xEjAQBgNVBAMTCUNsdXN0ZXJD\n" +
                "QTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKbe9/1uQWfHNKRKMUxX\n" +
                "xTu13mu80CDcLv7ydTgg44Kzf7TnGWrmESuISceyZyfUCzXsGItByzrb78hz6tj6\n" +
                "dv/8N6kpj1yTAj0DqwdTCRhbyUvK/C+0rgi5YkBIMN+ZdihEA99kfo2mJEZatLFr\n" +
                "r//MC0SS5DclrkujaxL9oeuET2764oFNvsoz43dS4m/iIlEEXgvIa/wugluJNfbb\n" +
                "FPAxL+YoMCuf2ybfKKy8Eo5dPh14AOtfL5uyVE70JXDK1LqktBtdHQRETOvNZPEI\n" +
                "bj9FUZM4vVD34vXXWwLjTnx1o1PttS8mj8p5TMtpqXT4toLaxFevgOiSoqAIbtMJ\n" +
                "cbUCAwEAAaNjMGEwDgYDVR0PAQH/BAQDAgIEMA8GA1UdEwEB/wQFMAMBAf8wHQYD\n" +
                "VR0OBBYEFHdcdKdarpL368/s4jGpRhRHpEvDMB8GA1UdIwQYMBaAFA9Fok9aepFa\n" +
                "HaAO4xlJjp29rmcSMA0GCSqGSIb3DQEBCwUAA4IBAQB3WS4IP4WPgq/y3PHzAhq6\n" +
                "OpTx17DjxscZZ8gox9PDGRXK5IkkDEa+waRTq1DaEwL7XV+nHWSo77QxsIuk/m4h\n" +
                "sVKUtJ5muJ/x23iHKesq4Kr+GWMKdOd9LSftQEkF7L97sLl+51cwcrCbYN47cHt7\n" +
                "bfZwJ2C+hqEpuLGtGREXzsGwe27ta8OC8gfJV2cMd9QtVj7QOCFvzPTpIRqEFVb2\n" +
                "mV3XZnvI+FtNtuP86wNLYP1YDFIhEt0G+KBY4BIgI1VjOFJKAVpDCBoNO2+TUE0a\n" +
                "UJ7ftdxyKWnytI2fezgtAFEFJzE0HMLXPORDU4cxOfXfFQRteFRAl/Tzju/iQbTv\n" +
                "-----END CERTIFICATE-----"));
    }
}
