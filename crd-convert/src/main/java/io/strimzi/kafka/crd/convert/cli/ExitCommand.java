/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import picocli.CommandLine;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("DM_EXIT")
@CommandLine.Command(name = "exit", aliases = {"x"}, description = "Exit shell")
public class ExitCommand implements Runnable {
    @Override
    public void run() {
        System.exit(0);
    }
}
