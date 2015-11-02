/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.ingestion.dist;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LaunchIngestion {

    private static final Logger logger = LoggerFactory.getLogger(LaunchIngestion.class);

    private static String APP_NAME= "LaunchIngestion";
    private static Options options;
    private static CommandLine commandLine;

    private static String zkQuorum;
    private static String workflowId;
    private static boolean forceDownload= false;


    public static void main(String[] args) throws Exception {
        parseOptions(args);
    }

    private static void parseOptions(String[] args)   {

        options = new Options();

        // Zookeeper options
        Option option = new Option ("zk", "zookeeper", true, "Zookeeper quorum");
        option.setRequired(true);
        options.addOption(option);

        // Workflow id options
        option = new Option ("id", "workflow-id", true, "Workflow Id to execute");
        option.setRequired(true);
        options.addOption(option);

        option = new Option ("f", "force", true, "Force download again of workflow ignoring existing files");
        option.setRequired(false);
        options.addOption(option);

        option.setRequired(false);
        options.addOption(option);
        option = new Option("h", "help", false, "display help text");
        options.addOption(option);


        GnuParser parser = new GnuParser();
        try {
            commandLine = parser.parse(options, args);
        } catch( ParseException e ) {
            logger.error( "Parsing failed.  Reason: " + e.getMessage() );
            printHelp(options);
            System.exit(1);
        }

        if (!commandLine.hasOption("zk") || !commandLine.hasOption("id")) {
            printHelp(options);
            System.exit(1);
        }
        zkQuorum = commandLine.getOptionValue("zk");
        workflowId= commandLine.getOptionValue("id");

        if (commandLine.hasOption("f")) {
            forceDownload= Boolean.parseBoolean(commandLine.getOptionValue("f"));
        }

        logger.info("Using parameters: ZookeeperQuorum = " + zkQuorum + ", WorkflowId = " + workflowId);
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( APP_NAME, options );
    }


}
