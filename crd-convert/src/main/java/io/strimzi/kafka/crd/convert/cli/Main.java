/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import io.strimzi.api.annotations.ApiVersion;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import org.apache.logging.log4j.Level;
import picocli.CommandLine;
import picocli.shell.jline2.PicocliJLineCompleter;

import java.util.HashMap;
import java.util.Map;

/**
 * Main to run as interactive shell.
 */
public class Main {
    public static void main(String[] args) {
        try {
            ConsoleReader reader = new ConsoleReader();
            CommandLine.IFactory factory = new CachingFactory(CommandLine.defaultFactory());

            // set up the completion
            MainCommand commands = new MainCommand(reader);
            CommandLine cmd = new CommandLine(commands, factory);
            cmd.registerConverter(Level.class, Level::toLevel);
            cmd.registerConverter(ApiVersion.class, ApiVersion::parse);
            reader.addCompleter(new PicocliJLineCompleter(cmd.getCommandSpec()));

            // start the shell and process input until the user quits with Ctl-D or exit/x
            reader.getOutput().append("Welcome to Strimzi CRD CLI\n");
            String line;
            while ((line = reader.readLine("$> ")) != null) {
                ArgumentCompleter.ArgumentList list = new ArgumentCompleter.WhitespaceArgumentDelimiter()
                        .delimit(line, line.length());
                cmd.execute(list.getArguments());
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private static class CachingFactory implements CommandLine.IFactory {
        private final Map<Class<?>, Object> instances = new HashMap<>();

        private final CommandLine.IFactory factory;

        public CachingFactory(CommandLine.IFactory factory) {
            this.factory = factory;
        }

        @Override
        public <K> K create(Class<K> cls) {
            Object result = instances.computeIfAbsent(cls, clazz -> {
                try {
                    return factory.create(clazz);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            return cls.cast(result);
        }
    }
}
