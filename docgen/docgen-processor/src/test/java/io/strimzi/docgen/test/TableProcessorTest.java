/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.test;

import io.strimzi.docgen.processor.TableProcessor;
import org.junit.Test;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;

import static java.util.Arrays.asList;

public class TableProcessorTest {

    @Test
    public void test0() throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> cus = fileManager.getJavaFileObjectsFromFiles(asList(
                new File("src/test/java/example/EnvVar.java"),
                new File("src/test/java/example/UseSite.java")));
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostic -> {
                    //throw new RuntimeException(diagnostic.getMessage(null));
                },
                null, asList("example.UseSite"), cus);
        task.setProcessors(asList(new TableProcessor()));
        task.call();
        fileManager.close();
    }
}
