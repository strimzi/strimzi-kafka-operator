/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtestdoc;

import io.strimzi.systemtestdoc.annotations.TestDoc;
import io.strimzi.systemtestdoc.markdown.Header;
import io.strimzi.systemtestdoc.markdown.Table;
import io.strimzi.systemtestdoc.markdown.TextList;
import io.strimzi.systemtestdoc.markdown.TextStyle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DocGenerator {

    private static final Pattern REMOVE_BEFORE_PACKAGE = Pattern.compile(".*java\\/");

    public static void generate(Class<?> testClass, String classFilePath) throws IOException {
        List<Method> methods = Arrays.stream(testClass.getDeclaredMethods())
                .filter(method -> method.getAnnotation(TestDoc.class) != null)
                .toList();

        if (!methods.isEmpty()) {
            String fileName = classFilePath.substring(classFilePath.lastIndexOf('/') + 1);
            String parentPath = classFilePath.replace(fileName, "");

            final File parent = new File(parentPath);
            if (!parent.mkdirs()) {
                System.err.println("Could not create parent directories ");
            }
            final File classFile = new File(parent, fileName);
            classFile.createNewFile();

            FileWriter write = new FileWriter(classFile);
            PrintWriter printWriter = new PrintWriter(write);

            printWriter.println(Header.firstLevelHeader(testClass.getSimpleName()));

            methods.forEach(method -> {
                TestDoc testDoc = method.getAnnotation(TestDoc.class);
                if (testDoc != null) {
                    createRecord(printWriter, testDoc, method.getName());
                }
            });

            printWriter.close();
        }
    }

    public static void createRecord(PrintWriter write, TestDoc testDoc, String methodName) {
        write.println();
        write.println(Header.secondLevelHeader(methodName));
        write.println();
        write.println(TextStyle.boldText("Description:") + " " + testDoc.description().value());
        write.println();
        write.println(TextStyle.boldText("Steps:"));
        write.println();
        write.println(createTableOfSteps(testDoc.steps()));
        write.println(TextStyle.boldText("Use-cases:"));
        write.println();
        write.println(TextList.createUnorderedList(createUseCases(testDoc.usecases())));
    }

    private static String createTableOfSteps(TestDoc.Step[] steps) {
        List<String> tableRows = new ArrayList<>();
        List<String> headers = List.of("Step", "Action", "Result");

        for (int i = 0; i < steps.length; i++) {
            tableRows.add(Table.createRow(i + 1 + ".", steps[i].value(), steps[i].expected()));
        }

        return Table.createTable(headers, tableRows);
    }

    private static List<String> createUseCases(TestDoc.Usecase[] usecases) {
        List<String> usesText = new ArrayList<>();
        Arrays.stream(usecases).forEach(usecase -> usesText.add("`" + usecase.id() + "`"));

        return usesText;
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        CommandLineOptions cmdOptions = new CommandLineOptions(args);
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        Map<String, String> classes = getTestClassesWithTheirPath(cmdOptions.getFilePath(), cmdOptions.getGeneratePath());

        for (Map.Entry<String, String> entry : classes.entrySet()) {
            Class<?> testClass = classLoader.loadClass(entry.getValue());

            generate(testClass, entry.getKey() + ".md");
        }
    }

    private static Map<String, String> getTestClassesWithTheirPath(String filePath, String generatePath) {
        Map<String, String> classes = new HashMap<>();

        try {
            Files.list(Paths.get(filePath))
                .filter(file -> !file.getFileName().toString().contains("AbstractST"))
                .forEach(path -> {
                    classes.putAll(getClassesForPackage(classes, path, generatePath));
                });
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }

        return classes;
    }

    private static Map<String, String> getClassesForPackage(Map<String, String> classes, Path packagePath, String generatePath) {
        try {
            Files.list(packagePath)
                .forEach(path -> {
                    if (Files.isDirectory(path)) {
                        classes.putAll(getClassesForPackage(classes, path, generatePath));
                    } else {
                        String classPackagePath = path.toAbsolutePath().toString().replaceAll(REMOVE_BEFORE_PACKAGE.toString(), "").replace(".java", "");
                        classes.put(generatePath + classPackagePath, classPackagePath.replaceAll("/", "."));
                    }
                });
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }

        return classes;
    }

}
