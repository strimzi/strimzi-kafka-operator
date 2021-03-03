/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.Level;
import picocli.CommandLine;

/**
 * The converter cli entry point
 */
@SuppressFBWarnings("ISC_INSTANTIATE_STATIC_CLASS")
@CommandLine.Command(
        name = "bin/api-conversion.sh",
        description = "Conversion tool for Strimzi Custom Resources",
        mixinStandardHelpOptions = true,
        version = "1.0",
        subcommands = {
                CommandLine.HelpCommand.class,
                ConvertFileCommand.class,
                ConvertResourceCommand.class,
                CrdUpgradeCommand.class
        }
)
class EntryCommand {
    public static void main(String[] args) {
        CommandLine cmd = new CommandLine(new EntryCommand());
        cmd.registerConverter(Level.class, Level::toLevel);
        int exit = cmd.execute(args);
        System.exit(exit);
    }
}