/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "demo", headerHeading = XapMainCommand.HEADER, header = "Runs a demo Partitioned Space with two partitions and backups")
public class DemoCommand extends CliCommand {

    private final String SPACE_NAME = "demo-space";
    private final boolean HA = true;
    private final int PARTITIONS_COUNT = 2;

    //xap space run --partitions=2 --lus --ha demo-space
    // xap demo ^
    @Override
    protected void execute() throws Exception {

        final List<ProcessBuilder> processBuilders = new ArrayList<ProcessBuilder>();
        processBuilders.add(AbstractRunCommand.buildStartLookupServiceCommand());
        processBuilders.add(SpaceRunCommand.buildPartitionedSpaceCommand(1, SPACE_NAME, HA, PARTITIONS_COUNT));
        processBuilders.add(SpaceRunCommand.buildPartitionedBackupSpaceCommand(1, SPACE_NAME, HA, PARTITIONS_COUNT));
        processBuilders.add(SpaceRunCommand.buildPartitionedSpaceCommand(2, SPACE_NAME, HA, PARTITIONS_COUNT));
        processBuilders.add(SpaceRunCommand.buildPartitionedBackupSpaceCommand(2, SPACE_NAME, HA, PARTITIONS_COUNT));
        XapCliUtils.executeProcesses(processBuilders);

    }
}
