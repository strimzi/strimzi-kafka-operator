/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;

@CommandLine.Command(name = "clear", aliases = {"cls"}, description = "Clear console")
public class ClearCommand implements Runnable {
    @CommandLine.ParentCommand
    MainCommand parent;

    public void run() {
        try {
            parent.reader.clearScreen();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
