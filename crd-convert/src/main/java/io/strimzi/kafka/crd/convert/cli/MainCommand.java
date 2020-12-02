/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import jline.console.ConsoleReader;
import picocli.CommandLine;

/**
 * The converter cli main command
 */
@CommandLine.Command(
        name = "crd",
        description = "Main shell command",
        mixinStandardHelpOptions = true,
        version = "1.0",
        subcommands = {
                CommandLine.HelpCommand.class,
                ConvertCommand.class,
                ClearCommand.class,
                ExitCommand.class
        }
)
class MainCommand {
    ConsoleReader reader;

    public MainCommand(ConsoleReader reader) {
        this.reader = reader;
    }
}