/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.imru.ec2;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.imru.util.R;

/**
 * @author wangrui
 */
public class ImruEc2Cmd {
    public static class Options {
        @Option(name = "-credentials-file", usage = "http://aws.amazon.com/security-credentials", required = true)
        public String credentialsFile;

        @Option(name = "-key-file", usage = "Key pair in https://console.aws.amazon.com/ec2/", required = true)
        public String keyFile;

        @Option(name = "-full-stack-root", usage = "local directory of svn co https://hyracks.googlecode.com/svn/branches/fullstack_imru", required = true)
        public String root;

        @Option(name = "-cluster-prefix", usage = "Unique prefix of each instance name belong to this cluster", required = true)
        public String clusterPrefix;

        @Option(name = "-instance-type", usage = "instance type (t1.micro, ...)")
        public String instanceType = "t1.micro";

        @Option(name = "-image-id", usage = "image used to start instance")
        public String imageId = HyracksEC2Cluster.FULLSTACK_IMRU_IMAGE_ID;

        @Option(name = "-max-instances", usage = "Maximum instances allowed")
        public int maxInstances = 5;

        @Option(name = "-cmd", usage = "command to execute", required = true)
        public String cmd;
    }

    Options options = new Options();
    CmdLineParser parser = new CmdLineParser(options);
    File imruRoot;
    HyracksEC2Cluster cluster;

    public ImruEc2Cmd(String[] args) throws Exception {
        int count = 0;
        for (int i = args.length - 1; i > 0; i--) {
            if ("-cmd".equals(args[i])) {
                if (i + 2 < args.length)
                    count = Integer.parseInt(args[i + 2]);
                args = Arrays.copyOf(args, i + 2);
                break;
            }
        }
        if (args.length == 0) {
            printUsage(true);
            return;
        }
        parser.parseArgument(args);
        if (options.cmd == null) {
            printUsage(false);
            return;
        }
        File credentialsFile = new File(options.credentialsFile);
        if (!credentialsFile.exists())
            throw new IOException(
                    credentialsFile.getAbsolutePath()
                            + " doesn't exist.\r\n"
                            + "Insert your AWS Credentials from http://aws.amazon.com/security-credentials to a file with content\r\naccessKey=xx\r\n"
                            + "secretKey=xx");
        File privateKey = new File(options.keyFile);
        if (!privateKey.exists())
            throw new Error(options.keyFile + " doesn't exist. Please create "
                    + "a key pair in https://console.aws.amazon.com/ec2/ and download it to "
                    + privateKey.getParentFile().getAbsolutePath() + "/");
        cluster = new HyracksEC2Cluster(credentialsFile, privateKey, options.clusterPrefix);
        cluster.setMachineType(options.instanceType);
        cluster.setImageId(options.imageId);
        this.imruRoot = new File(options.root);
        if ("status".equalsIgnoreCase(options.cmd) || "s".equalsIgnoreCase(options.cmd)) {
            cluster.printNodeStatus();
            R.np("Admin URL: " + cluster.getAdminURL());
        } else if ("install".equalsIgnoreCase(options.cmd)) {
            cluster.createSecurityGroup();
            cluster.install(imruRoot);
        } else if ("startInstances".equalsIgnoreCase(options.cmd) || "sti".equalsIgnoreCase(options.cmd)) {
            cluster.startInstances();
        } else if ("stopInstances".equalsIgnoreCase(options.cmd) || "spi".equalsIgnoreCase(options.cmd)) {
            cluster.stopInstances();
        } else if ("terminateInstances".equalsIgnoreCase(options.cmd) || "termi".equalsIgnoreCase(options.cmd)) {
            cluster.terminateInstances();
        } else if ("setInstanceCount".equalsIgnoreCase(options.cmd) || "sic".equalsIgnoreCase(options.cmd)) {
            cluster.setTotalInstances(count);
        } else if ("addInstances".equalsIgnoreCase(options.cmd) || "addi".equalsIgnoreCase(options.cmd)) {
            cluster.addInstances(count);
        } else if ("startHyracks".equalsIgnoreCase(options.cmd) || "sth".equalsIgnoreCase(options.cmd)) {
            cluster.startHyrackCluster();
        } else if ("stopHyracks".equalsIgnoreCase(options.cmd) || "sph".equalsIgnoreCase(options.cmd)) {
            cluster.stopHyrackCluster();
        } else if ("logs".equals(options.cmd)) {
            cluster.printLogs();
        } else {
            System.out.println("Unknown command: " + options.cmd);
            printUsage(false);
        }
    }

    void printUsage(boolean showOptions) {
        PrintStream p = System.out;
        p.println("Hyracks EC2 controller");
        if (showOptions) {
            p.println("Options: ");
            parser.printUsage(System.out);
        }
        p.println();
        p.println("Available commands: ");
        p.println(" s|status                     - print status of all instances");
        p.println(" sic|setInstanceCount <count> - add/remote instances to reach <count>");
        p.println(" addi|addInstances <count>    - add <count> instances");
        p.println(" install                      - install hyracks to all instances");
        p.println(" sti|startInstances           - start all instances");
        p.println(" sth|startHyrack              - start hyracks on all instances");
        p.println(" sph|stopHyracks              - stop hyracks on all instances");
        p.println(" spi|stopInstances            - stop all instances");
        p.println(" termi|terminateInstances     - terminate all instances");
        p.println(" logs                         - show hyracks logs on all instances");
    }

    public static void main(String[] args) throws Exception {
        if (false) {
            File home = new File(System.getProperty("user.home"));
            File credentialsFile = new File(home, "AwsCredentials.properties");
            if (!credentialsFile.exists())
                throw new IOException(
                        credentialsFile.getAbsolutePath()
                                + " doesn't exist.\r\n"
                                + "Insert your AWS Credentials from http://aws.amazon.com/security-credentials to a file with content\r\naccessKey=xx\r\n"
                                + "secretKey=xx");
            File imruRoot = new File(home, "fullstack_imru");
            String keyName = "firstTestByRui";
            File privateKey = new File(home, keyName + ".pem");
            if (!privateKey.exists())
                throw new Error("Key pair needed. Please create "
                        + "a key pair in https://console.aws.amazon.com/ec2/ and download it to "
                        + home.getAbsolutePath());
            args = new String[] { "-credentials-file", credentialsFile.getAbsolutePath(),//
                    "-key-file", privateKey.getAbsolutePath(), //
                    "-full-stack-root", imruRoot.getAbsolutePath(), //
                    "-cluster-prefix", "IMRU-auto-deploy-", //
                    "-max-instances", "3", "-cmd", "status", "2" };
        }
        new ImruEc2Cmd(args);
    }
}
