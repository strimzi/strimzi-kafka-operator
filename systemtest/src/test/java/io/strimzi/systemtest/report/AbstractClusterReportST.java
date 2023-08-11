/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.utils.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.USER_PATH;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag(REGRESSION)
public abstract class AbstractClusterReportST extends AbstractST {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    protected String buildOutPath(TestInfo testInfo, String clusterName) {
        String methodName = testInfo.getTestMethod().isPresent() ?
            testInfo.getTestMethod().get().getName() : UUID.randomUUID().toString();
        return USER_PATH + "/target/reports/" + clusterName + "/" + methodName;
    }

    protected <T> void assertValidYamls(String path, Class<T> clazz, String prefix, int num) throws IOException {
        final File[] files = FileUtils.listFilesWithPrefix(path, prefix);
        assertNotNull(files);
        assertThat(format("Missing files of type %s with prefix %s", clazz.getSimpleName(), prefix), files.length == num);
        for (File file : files) {
            String fileNameWithoutExt = file.getName().replaceFirst("[.][^.]+$", "");
            HasMetadata resource = (HasMetadata) MAPPER.readValue(file, clazz);
            assertThat(resource.getMetadata().getName(), is(fileNameWithoutExt));
        }
    }

    protected void assertValidFiles(String path, String prefix, int num) {
        final File[] files = FileUtils.listFilesWithPrefix(path, prefix);
        assertNotNull(files);
        assertThat(format("Missing files with prefix '%s'", prefix), files.length == num);
        for (File file : files) {
            assertThat(format("%s is empty", file.getAbsolutePath()), file.length(), greaterThan(0L));
        }
    }
}
