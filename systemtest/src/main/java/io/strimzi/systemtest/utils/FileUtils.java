/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileUtils {

    private static final Logger LOGGER = LogManager.getLogger(FileUtils.class);

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static File downloadAndUnzip(String url) {
        File dir = null;
        FileOutputStream fout = null;
        ZipInputStream zin = null;
        try {
            InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
            dir = Files.createTempDirectory(FileUtils.class.getName()).toFile();
            dir.deleteOnExit();
            zin = new ZipInputStream(bais);
            ZipEntry entry = zin.getNextEntry();
            byte[] buffer = new byte[8 * 1024];
            int len;
            while (entry != null) {
                File file = new File(dir, entry.getName());
                if (entry.isDirectory()) {
                    file.mkdirs();
                } else {
                    fout = new FileOutputStream(file);
                    while ((len = zin.read(buffer)) != -1) {
                        fout.write(buffer, 0, len);
                    }
                    fout.close();
                }
                entry = zin.getNextEntry();
            }
        } catch (IOException e) {
            LOGGER.error("IOException {}", e.getMessage());
        } finally {
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    LOGGER.error("IOException {}", e.getMessage());
                }
            }
            if (zin != null) {
                try {
                    zin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return dir;
    }

    public static File downloadYamlAndReplaceNameSpace(String url, String namespace) {
        InputStream bais = null;
        BufferedReader br = null;
        OutputStreamWriter osw = null;

        try {
            bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
            StringBuilder sb = new StringBuilder();
            br = new BufferedReader(new InputStreamReader(bais, StandardCharsets.UTF_8));
            String read;
            while ((read = br.readLine()) != null) {
                sb.append(read + "\n");
            }
            br.close();
            String yaml = sb.toString();
            File yamlFile = File.createTempFile("temp-file", ".yaml");
            osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8);
            yaml = yaml.replaceAll("namespace: .*", "namespace: " + namespace);
            yaml = yaml.replace("securityContext:\n" +
                "        runAsNonRoot: true\n" +
                "        runAsUser: 65534", "");
            osw.write(yaml);
            osw.close();
            return yamlFile;

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bais != null) {
                try {
                    bais.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (osw != null) {
                try {
                    osw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        return null;
    }

    public static File updateNamespaceOfYamlFile(String pathToOrigin, String namespace) {
        byte[] encoded;
        OutputStreamWriter osw = null;

        try {
            encoded = Files.readAllBytes(Paths.get(pathToOrigin));

            String yaml = new String(encoded, StandardCharsets.UTF_8);
            yaml = yaml.replaceAll("namespace: .*", "namespace: " + namespace);

            File yamlFile = File.createTempFile("temp-file", ".yaml");
            osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8);
            osw.write(yaml);
            osw.close();
            return yamlFile.toPath().toFile();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (osw != null) {
                try {
                    osw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
