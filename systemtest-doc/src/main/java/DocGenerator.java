/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
import io.strimzi.annotations.TestDoc;
import io.strimzi.markdown.Header;
import io.strimzi.markdown.Table;
import io.strimzi.markdown.TextList;
import io.strimzi.markdown.TextStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import java.util.List;

public class DocGenerator {

    private static final String FILE_PATH = "my-file.md";

    public static void generate(Class<?> testClass) throws IOException {
        Path filePath = Paths.get(FILE_PATH);
        Files.createFile(filePath);

        File myFile = new File(FILE_PATH);
        FileWriter write = new FileWriter(myFile);
        PrintWriter printWriter = new PrintWriter(write);

        printWriter.println(Header.firstLevelHeader(testClass.getName()));

        Method[] methods = testClass.getDeclaredMethods();

        Arrays.stream(methods).forEach( method -> {
            TestDoc testDoc =method.getAnnotation(TestDoc.class);
            createRecord(printWriter, testDoc, method.getName());
        });

        printWriter.close();
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

}
