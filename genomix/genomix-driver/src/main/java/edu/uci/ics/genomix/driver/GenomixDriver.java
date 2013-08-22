/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.genomix.driver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.minicluster.GenomixMiniCluster;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.bubblemerge.BubbleMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P1ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P2ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.removelowcoverage.RemoveLowCoverageVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.ScaffoldingVertex;
import edu.uci.ics.genomix.pregelix.operator.splitrepeat.SplitRepeatVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat.UnrollTandemRepeat;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 */
public class GenomixDriver {
    private String prevOutput;
    private String curOutput;
    private int stepNum;
    private List<PregelixJob> jobs;
    private boolean followingBuild = false; // need to adapt the graph immediately after building

    private edu.uci.ics.genomix.hyracks.graph.driver.Driver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(
            this.getClass());

    private void copyLocalToHDFS(JobConf conf, String localDir, String destDir) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        Path dest = new Path(destDir);
        dfs.delete(dest, true);
        dfs.mkdirs(dest);

        File srcBase = new File(localDir);
        if (srcBase.isDirectory())
            for (File f : srcBase.listFiles())
                dfs.copyFromLocalFile(new Path(f.toString()), dest);
        else
            dfs.copyFromLocalFile(new Path(localDir), dest);
    }

    private static void copyBinToLocal(GenomixJobConf conf, String hdfsSrcDir, String localDestDir) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        FileUtils.deleteQuietly(new File(localDestDir));

        // save original binary to output/bin
        dfs.copyToLocalFile(new Path(hdfsSrcDir), new Path(localDestDir + File.separator + "bin"));

        // convert hdfs sequence files to text as output/text
        BufferedWriter bw = null;
        SequenceFile.Reader reader = null;
        Writable key = null;
        Writable value = null;
        FileStatus[] files = dfs.globStatus(new Path(hdfsSrcDir + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    if (bw == null)
                        bw = new BufferedWriter(new FileWriter(localDestDir + File.separator + "text"));
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        bw.write(key.toString() + "\t" + value.toString());
                        bw.newLine();
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error copying " + f + " to local:\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
                
            }
        }
        if (bw != null)
            bw.close();
    }

    private void buildGraph(GenomixJobConf conf) throws NumberFormatException, HyracksException {
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(conf.get(GenomixJobConf.IP_ADDRESS),
                Integer.parseInt(conf.get(GenomixJobConf.PORT)), Integer.parseInt(conf
                        .get(GenomixJobConf.CPARTITION_PER_MACHINE)));
        hyracksDriver.runJob(conf, Plan.BUILD_UNMERGED_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
        followingBuild = true;
    }

    private void setOutput(GenomixJobConf conf, Patterns step) {
        prevOutput = curOutput;
        curOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("%02d-", stepNum) + step;
        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
    }

    private void addJob(PregelixJob job) {
        if (followingBuild)
            job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        jobs.add(job);
        followingBuild = false;
    }

    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException, Exception {
        KmerBytesWritable.setGlobalKmerLength(Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)));
        jobs = new ArrayList<PregelixJob>();
        stepNum = 0;
        boolean runLocal = Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL));
        if (runLocal)
            GenomixMiniCluster.init(conf);

        String localInput = conf.get(GenomixJobConf.LOCAL_INPUT_DIR);
        if (localInput != null) {
            conf.set(GenomixJobConf.INITIAL_INPUT_DIR, conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator
                    + "00-initial-input-from-genomix-driver");
            copyLocalToHDFS(conf, localInput, conf.get(GenomixJobConf.INITIAL_INPUT_DIR));
        }
        curOutput = conf.get(GenomixJobConf.INITIAL_INPUT_DIR);

        // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
        String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
        for (Patterns step : Patterns.arrayFromString(pipelineSteps)) {
            stepNum++;
            switch (step) {
                case BUILD:
                case BUILD_HYRACKS:
                    setOutput(conf, Patterns.BUILD);
                    buildGraph(conf);
                    break;
                case BUILD_MR:
                    //TODO add the hadoop build code
                    throw new IllegalArgumentException("BUILD_MR hasn't been added to the driver yet!");
                case MERGE_P1:
                    setOutput(conf, Patterns.MERGE_P1);
                    addJob(P1ForPathMergeVertex.getConfiguredJob(conf));
                    break;
                case MERGE_P2:
                    setOutput(conf, Patterns.MERGE_P2);
                    addJob(P2ForPathMergeVertex.getConfiguredJob(conf));
                    break;
                case MERGE:
                case MERGE_P4:
                    setOutput(conf, Patterns.MERGE_P4);
                    addJob(P4ForPathMergeVertex.getConfiguredJob(conf));
                    break;
                case UNROLL_TANDEM:
                    setOutput(conf, Patterns.UNROLL_TANDEM);
                    addJob(UnrollTandemRepeat.getConfiguredJob(conf));
                    break;
                case TIP_REMOVE:
                    setOutput(conf, Patterns.TIP_REMOVE);
                    addJob(TipRemoveVertex.getConfiguredJob(conf));
                    break;
                case BUBBLE:
                    setOutput(conf, Patterns.BUBBLE);
                    addJob(BubbleMergeVertex.getConfiguredJob(conf));
                    break;
                case LOW_COVERAGE:
                    setOutput(conf, Patterns.LOW_COVERAGE);
                    addJob(RemoveLowCoverageVertex.getConfiguredJob(conf));
                    break;
                case BRIDGE:
                    setOutput(conf, Patterns.BRIDGE);
                    addJob(BridgeRemoveVertex.getConfiguredJob(conf));
                    break;
                case SPLIT_REPEAT:
                    setOutput(conf, Patterns.SPLIT_REPEAT);
                    addJob(SplitRepeatVertex.getConfiguredJob(conf));
                    break;
                case SCAFFOLD:
                    setOutput(conf, Patterns.SCAFFOLD);
                    addJob(ScaffoldingVertex.getConfiguredJob(conf));
                    break;
            }
        }
        for (int i = 0; i < jobs.size(); i++) {
            //                pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(jobs.get(i).getConfiguration().getClass(PregelixJob.VERTEX_CLASS, null));
            //                pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(jobs.get(i).getConfiguration().getClass(PregelixJob.VERTEX_CLASS, null));
            pregelixDriver.runJob(jobs.get(i), conf.get(GenomixJobConf.IP_ADDRESS),
                    Integer.parseInt(conf.get(GenomixJobConf.PORT)));
        }

        //            pregelixDriver.runJobs(jobs, conf.get(GenomixJobConf.IP_ADDRESS), Integer.parseInt(conf.get(GenomixJobConf.PORT)));

        if (conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR) != null)
            copyBinToLocal(conf, curOutput, conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR));
        if (conf.get(GenomixJobConf.FINAL_OUTPUT_DIR) != null)
            FileSystem.get(conf).rename(new Path(curOutput), new Path(GenomixJobConf.FINAL_OUTPUT_DIR));

        if (runLocal)
            GenomixMiniCluster.deinit();
    }

    public static void main(String[] args) throws CmdLineException, NumberFormatException, HyracksException, Exception {
        String[] myArgs = { "-runLocal", "-kmerLength", "3",
                //                "-localInput", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/input/reads/synthetic/",
                "-localInput", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/input/reads/pathmerge",
                "-localOutput", "output",
                //                            "-pipelineOrder", "BUILD,MERGE",
                //                            "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-driver/graphbuild.binmerge",
                //                "-localInput", "../genomix-pregelix/data/TestSet/PathMerge/CyclePath/bin/part-00000", 
                "-pipelineOrder", "BUILD" };
        GenomixJobConf conf = GenomixJobConf.fromArguments(myArgs);
        GenomixDriver driver = new GenomixDriver();
        driver.runGenomix(conf);
    }
}
