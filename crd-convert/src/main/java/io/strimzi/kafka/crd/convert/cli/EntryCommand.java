/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.annotations.ApiVersion;
import org.apache.logging.log4j.Level;
import picocli.CommandLine;

/**
 * The converter cli entry point
 */
@SuppressFBWarnings("ISC_INSTANTIATE_STATIC_CLASS")
@CommandLine.Command(
        name = "crd",
        description = "Simple entry command",
        mixinStandardHelpOptions = true,
        version = "1.0",
        subcommands = {
                CommandLine.HelpCommand.class,
                ConvertCommand.class
        }
)
class EntryCommand {
    public static void main(String[] args) {
        CommandLine cmd = new CommandLine(new EntryCommand());
        cmd.registerConverter(Level.class, Level::toLevel);
        cmd.registerConverter(ApiVersion.class, ApiVersion::parse);
        int exit = cmd.execute(args);
        System.exit(exit);
    }
}