/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ClusterCaRenewalTest {
    private static final Function<NodeRef, Subject> SUBJECT_FN = node -> {
        Subject.Builder subject = new Subject.Builder();
        subject.addDnsName("example.local");
        subject.addIpAddress("127.0.0.1");
        return subject.build();
    };
    private static final Set<NodeRef> NODES = new LinkedHashSet<>();

    // This certificate is used for testing purposes only. It is valid until 2126 and includes SANs (DNS: example.local; IP: 127.0.0.1).
    private final static String DUMMY_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDTDCCAjSgAwIBAgIUWAVui97Rf8gVcC/s5QL+L4OL04cwDQYJKoZIhvcNAQEL\n" +
            "BQAwLTETMBEGA1UECgwKaW8uc3RyaW16aTEWMBQGA1UEAwwNY2x1c3Rlci1jYSB2\n" +
            "MDAgFw0yNjA3MDYwOTAyMzBaGA8yMTI2MDYxMjA5MDIzMFowFTETMBEGA1UEAwwK\n" +
            "dmFsaWQtdXNlcjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAO5t4wYz\n" +
            "10K39pRlxNy3LXa6nFsnRi7g+C2h5X1Hbu/9Lv2mSVy0MmLWNw/Nx8qbdprdgBlt\n" +
            "GnmEz2e435g6tg354qlyb/pMSEE6v1tXpY68+p882tDFY/+2y1YEcseQUXd70H7T\n" +
            "pT+Go/Bqj5e4YpsZPUtPw0NSDJAp0DYwcJFeVtrltv11CWX6A8p0LaqaTT/gewKP\n" +
            "D19Z6tQ/LOQpMHRXwcKpk8Xwuoj6iV5SuhYa/Tvmz6h/vedDftcUEVbmyQLXLyUf\n" +
            "o+4QLGyTtQqJ11B5ssKhAsB4DSd7WIfuZSUo8sykGeoVJ2bNF+RmQtvRBt0xYWgq\n" +
            "CoTrTgs2q7zsAU8CAwEAAaN6MHgwCQYDVR0TBAIwADALBgNVHQ8EBAMCBaAwHgYD\n" +
            "VR0RBBcwFYINZXhhbXBsZS5sb2NhbIcEfwAAATAdBgNVHQ4EFgQUcWpAdlVDH5RL\n" +
            "TAntb4q2Bqkt3YowHwYDVR0jBBgwFoAURtdYM1go6ehB9x21eBjupnzy1pAwDQYJ\n" +
            "KoZIhvcNAQELBQADggEBAEBQ0ptk3cMCLQNG8IT5kkSruvoxov+mDDh4YLLTp9qQ\n" +
            "+nfCwFDyaDrGeQ8aKds5VF53lLdZV/s6L2oxjVdysrI6FMd2asUubbME0BitPsrS\n" +
            "qwWdasACBpQUM8JwZURVQHA0fCfdAB6v3kds5kz4QZ22FQI0I6HQ9ZGqsYIpHrA7\n" +
            "ZoGmkRZHnDhqw7VGDkyRaCjxSu8hMaYgorFhMFnoRflbCh44lJcv16hudyvt+v4l\n" +
            "Mmat3ZFEVzjyiKA/bSQKveGfS9D+LQedVRitZq15qh4heoc78pdxDZV664nttAgp\n" +
            "dl8yPaW/n+jon4ZQ1CEW6NVh3YHDFGcs5KHgdSNuL80=\n" +
            "-----END CERTIFICATE-----\n";


    // Certificate used for expiration tests where actual expiration is needed. This certificate expires on 27th March 2023 and includes SANs (DNS: example.local; IP: 127.0.0.1).
    // But with correct configuration or renewal days before expiration, it can be used to trigger expiration,
    private final static String EXPIRED_DUMMY_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDRDCCAiygAwIBAgIUAw8AFcPvJkD5ijYTuT5KBt6sUX4wDQYJKoZIhvcNAQEL\n" +
            "BQAwLTETMBEGA1UECgwKaW8uc3RyaW16aTEWMBQGA1UEAwwNY2xpZW50cy1jYSB2\n" +
            "MDAeFw0yMjAzMjcxNTQyNTBaFw0yMzAzMjcxNTQyNTBaMA8xDTALBgNVBAMMBHVz\n" +
            "ZXIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDG+c0r8BqSSttN3glQ\n" +
            "u1rhodlB3+Q4XTLMmpX1kvzaeMHGLS/D6gmu2X1Ox7QJkb5Sv9pq7ufUuSFXzWPV\n" +
            "Kyi4BrrrQH5ysKoaeEY8oZFZZjsD0FmAsh7NTzXCy4rXzj+p3zCZvJ/W3njsBhuW\n" +
            "ed/Cpbx/fJEfD8IzK3qpjDBFYgkhxOMuObiEar49rbFVs4lsP9wu+NwXIOKufCAj\n" +
            "EdQxTkcqR4Qt88g/DfJN0tP4bro2gdvbF6tlbFidFV0hXwQoN1oEflWU3Wc8gry7\n" +
            "cRSA0myTJKORgtQrBsv4+i42RbcqOFyYgAdYqkh4s/BQQHCHsU8uCewPA4Cc/xkV\n" +
            "kTFnAgMBAAGjejB4MAkGA1UdEwQCMAAwCwYDVR0PBAQDAgWgMB4GA1UdEQQXMBWC\n" +
            "DWV4YW1wbGUubG9jYWyHBH8AAAEwHQYDVR0OBBYEFCEHtiyjUqOmlZjAEOdNhpoU\n" +
            "QAb+MB8GA1UdIwQYMBaAFPEx+LWO3ZmV78yRt+1BMu/LB9LmMA0GCSqGSIb3DQEB\n" +
            "CwUAA4IBAQAK3NIyn7jBDniyTVkNvC0RMO4SqIAU7pzwFo575YrUizweYc7xdV2V\n" +
            "23zR9M4gIfom1Cjccws8ZgvHUsNR1UidewyEt4pihsiNB2ad6W6Ft+4CKbHTFd0g\n" +
            "Geu1S58duUJhIPHzRHkSYKGHqt0N6WYwYAgsWH4rJlL59Rkpv9IokW2C6ST2102H\n" +
            "0q7axKbD8nWiXpTfID8ei723u1imNKEg/2z60CgJPfDg8DscbhaoRpgND0XlcB3J\n" +
            "xxKC9M18nAM4kikCp0Cx4UvQbwdzNwjmDfQpmTwLCOfC88ZYYgEju0+rjna5caOb\n" +
            "2CTLAx8SYKOGBTzflSuvKzib0gSoXZUW\n" +
            "-----END CERTIFICATE-----\n";

    // LinkedHashSet is used to maintain ordering and have predictable test results
    static {
        NODES.add(new NodeRef("pod0", 0, null, false, true));
        NODES.add(new NodeRef("pod1", 1, null, false, true));
        NODES.add(new NodeRef("pod2", 2, null, false, true));
    }

    @Test
    public void renewalOfCertificatesWithNullCertificates() throws IOException {
        ClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                null,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @Test
    public void renewalOfCertificatesWithCaRenewal() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();
        mockedCa.setCaCertGeneration(1);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));
        assertThat(newCerts.get("pod0").caCertGeneration(), is(1));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(1));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
        assertThat(newCerts.get("pod2").caCertGeneration(), is(1));
    }

    @Test
    public void renewalOfCertificatesDelayedRenewalInWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));
        assertThat(newCerts.get("pod0").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
        assertThat(newCerts.get("pod2").caCertGeneration(), is(0));

    }

    @Test
    public void renewalOfCertificatesDelayedRenewalOutsideWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                false,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(newCerts.get("pod0").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod1").cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod2").cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(0));

    }

    @Test
    public void renewalOfCertificatesWithNewNodesOutsideWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                false,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key0"));
    }

    @Test
    public void noRenewalOfCertificates() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod2").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
    }

    @Test
    public void nosRenewalOfCertificatesWithScaleUp() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key0"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key1"));
    }

    @Test
    public void noRenewalOfCertificatesWithScaleUpInTheMiddle() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key0"));

        assertThat(new String(newCerts.get("pod2").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
    }

    @Test
    public void noRenewalOfCertificatesScaleDown() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                Set.of(new NodeRef("pod1", 1, null, false, true)),
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(newCerts.get("pod0"), is(nullValue()));

        assertThat(new String(newCerts.get("pod1").cert()), is(DUMMY_CERT));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));

        assertThat(newCerts.get("pod2"), is(nullValue()));
    }

    @Test
    public void changedSubjectOfCertificates() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                node -> new Subject.Builder().addDnsName(node.podName() + ".test.com").build(),
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
    }


    @Test
    public void certificatesIncludeCaChain() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                null,
                true,
                true
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0CA-CERT"));
        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1CA-CERT"));
        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2CA-CERT"));
    }

    @Test
    public void caChainAddedToExistingCertificates() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("new-key0".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod1", new CertAndKey("new-key1".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));
        initialCerts.put("pod2", new CertAndKey("new-key2".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                true
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0CA-CERT"));
        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1CA-CERT"));
        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2CA-CERT"));
    }

    @Test
    public void testRenewalOfDeploymentCertificateWithNullCertAndKey() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                null,
                true
        );

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testRenewalOfDeploymentCertificateWithRenewingCa() {
        MockedClusterCa mockedCa = new MockedClusterCa();
        mockedCa.setCaCertGeneration(1);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        );

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(1));
    }

    @Test
    public void testRenewalOfDeploymentCertificateDelayedRenewal() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        );

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testRenewalOfDeploymentCertificateDelayedRenewalOutsideOfMaintenanceWindow() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                false
        );

        assertThat(new String(newCert.cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testHandlingOldSecretWithPKCS12Files() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8),
                null, "old-keystore".getBytes(), "old-password");
        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        );

        assertThat(new String(newCert.cert()), is(DUMMY_CERT));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(new String(newCert.keyStore()), is("old-keystore"));
        assertThat(newCert.storePassword(), is("old-password"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    public static class MockedClusterCa extends ClusterCa {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private int caCertGeneration;

        public MockedClusterCa() {
            super(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null);
        }

        @Override
        public byte[] currentCaCertBytes() {
            return "CA-CERT".getBytes();
        }

        @Override
        protected CertAndKey generateSignedCert(Subject subject,
                                                File csrFile, File keyFile, File certFile, File keyStoreFile, boolean includeCaChain) {
            int index = invocationCount.getAndIncrement();

            byte[] cert;
            if (includeCaChain) {
                // Simulate concatenated chain: leaf + CA
                cert = ("new-cert" + index + "CA-CERT").getBytes();
            } else {
                cert = ("new-cert" + index).getBytes();
            }

            return new CertAndKey(
                    ("new-key" + index).getBytes(),
                    cert,
                    ("new-truststore" + index).getBytes(),
                    ("new-keystore" + index).getBytes(),
                    "new-password" + index,
                    caCertGeneration
            );
        }

        @Override
        public int caCertGeneration() {
            return caCertGeneration;
        }

        public void setCaCertGeneration(int value) {
            caCertGeneration = value;
        }
    }
}