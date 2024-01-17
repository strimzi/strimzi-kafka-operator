/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * The `OpenSsl` class encapsulates OpenSSL command execution using the OpenSSLCommand object,
 * which interfaces with the command-line version of OpenSSL. It serves as a versatile tool
 * for various OpenSSL operations, primarily focusing on the creation of private keys, the
 * generation of certificate signing requests (CSRs), and the signing of these CSRs using
 * a certificate authority (CA). The primary use case for this class is to facilitate the
 * simulation of externally provided client certificates, offering a seamless solution for
 * integrating secure authentication mechanisms into your application.
 */
public class OpenSsl {
    private static final Logger LOGGER = LogManager.getLogger(OpenSsl.class);

    private static class OpenSslCommand {
        ProcessBuilder pb = new ProcessBuilder();

        public OpenSslCommand(String command) {
            this("openssl", command);
        }

        public OpenSslCommand(String binary, String command) {
            pb.command().add(binary);
            pb.command().add(command);
        }

        public OpenSslCommand withOption(String option) {
            pb.command().add(option);
            return this;
        }

        public OpenSslCommand withOptionAndArgument(String option, File argument) {
            pb.command().add(option);
            pb.command().add(argument.getAbsolutePath());
            return this;
        }

        public OpenSslCommand withOptionAndArgument(String option, String argument) {
            pb.command().add(option);
            pb.command().add(argument);
            return this;
        }

        public void execute() {
            executeAndReturnOnSuccess(true);
        }

        public String executeAndReturn() {
            return executeAndReturnOnSuccess(true);
        }

        public String executeAndReturnOnSuccess(boolean failOnNonZeroOutput) {

            Path commandOutput = null;
            try {
                commandOutput = Files.createTempFile("openssl-command-output-", ".txt");

                pb.redirectErrorStream(true)
                    .redirectOutput(commandOutput.toFile());

                LOGGER.debug("Running command: {}", pb.command());

                Process process = pb.start();

                OutputStream outputStream = process.getOutputStream();
                outputStream.close();

                int exitCode = process.waitFor();
                String outputText = Files.readString(commandOutput, StandardCharsets.UTF_8);

                if (exitCode != 0 && failOnNonZeroOutput) {
                    throw new RuntimeException("Openssl command failed. " + outputText);
                }

                return outputText;
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                removeFile(commandOutput);
            }
        }

        static void removeFile(Path fileToRemove) {
            if (fileToRemove != null && Files.exists(fileToRemove)) {
                try {
                    Files.delete(fileToRemove);
                } catch (IOException e) {
                    LOGGER.debug("File could not be removed: {}", fileToRemove);
                }
            }

        }
    }

    public static File generatePrivateKey() {
        return generatePrivateKey(2048);
    }

    public static File generatePrivateKey(int keyLengthBits) {
        try {
            LOGGER.info("Creating client RSA private key with size of {} bits", keyLengthBits);
            File privateKey = Files.createTempFile("private-key-", ".pem").toFile();

            new OpenSslCommand("genpkey")
                    .withOptionAndArgument("-algorithm", "RSA")
                    .withOptionAndArgument("-pkeyopt", "rsa_keygen_bits:" + keyLengthBits)
                    .withOptionAndArgument("-out", privateKey)
                    .execute();

            return privateKey;
        } catch (IOException e)  {
            throw new RuntimeException(e);
        }
    }

    public static File generateCertSigningRequest(File privateKey, String subject) {
        try {
            LOGGER.info("Creating Certificate Signing Request file");
            File csr = Files.createTempFile("csr-", ".pem").toFile();

            new OpenSslCommand("req")
                .withOption("-new")
                .withOptionAndArgument("-key", privateKey)
                .withOptionAndArgument("-out", csr)
                .withOptionAndArgument("-subj", subject)
                .execute();

            return csr;
        } catch (IOException e)  {
            throw new RuntimeException(e);
        }
    }

    public static File generateSignedCert(File csr, File caCrt, File caKey) {
        try {
            LOGGER.info("Creating signed certificate file");
            File cert = Files.createTempFile("signed-cert-", ".pem").toFile();

            new OpenSslCommand("x509")
                .withOption("-req")
                .withOptionAndArgument("-in", csr)
                .withOptionAndArgument("-CA", caCrt)
                .withOptionAndArgument("-CAkey", caKey)
                .withOptionAndArgument("-out", cert)
                .withOption("-CAcreateserial")
                .execute();

            waitForCertIsInValidDateRange(cert);

            return cert;
        } catch (IOException e)  {
            throw new RuntimeException(e);
        }
    }

    public static void waitForCertIsInValidDateRange(File certificate) {
        String dates = new OpenSslCommand("x509")
            .withOption("-noout")
            .withOption("-dates")
            .withOptionAndArgument("-in", certificate)
            .executeAndReturn()
            .trim().replace("\s\s", "\s");

        String startDate = dates.split("\n")[0].replace("notBefore=", "");
        String endDate = dates.split("\n")[1].replace("notAfter=", "");

        ZoneId gmtZone = ZoneId.of("GMT");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM d[d] HH:mm:ss yyyy z");
        ZonedDateTime notBefore = ZonedDateTime.of(LocalDateTime.parse(startDate, formatter), gmtZone);
        ZonedDateTime notAfter = ZonedDateTime.of(LocalDateTime.parse(endDate, formatter), gmtZone);

        TestUtils.waitFor("certificate to be in valid date range", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.CO_OPERATION_TIMEOUT_SHORT,
                          () -> {
                ZonedDateTime now = ZonedDateTime.now(gmtZone);
                return (now.isAfter(notBefore.plusSeconds(TestConstants.CA_CERT_VALIDITY_DELAY)) && now.isBefore(notAfter.minusSeconds(TestConstants.CA_CERT_VALIDITY_DELAY)));
            });
    }
}
