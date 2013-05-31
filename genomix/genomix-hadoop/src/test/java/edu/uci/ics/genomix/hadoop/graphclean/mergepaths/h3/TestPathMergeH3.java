package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.genomix.hyracks.driver.Driver;
import edu.uci.ics.genomix.hyracks.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.test.TestUtils;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.hyracks.hdfs.utils.HyracksUtils;

@SuppressWarnings("deprecation")
public class TestPathMergeH3 {
    private static final int KMER_LENGTH = 5;
    private static final int READ_LENGTH = 8;
    
    private static final String LOCAL_SEQUENCE_FILE = "src/test/resources/data/webmap/text.txt";
    private static final String HDFS_SEQUENCE = "/00-sequence/";
    private static final String HDFS_GRAPHBUILD = "/01-graphbuild/";
    private static final String HDFS_MERGED = "/02-graphmerge/";

    private static final String EXPECTED_ROOT = "src/test/resources/expected/";
    private static final String ACTUAL_ROOT = "src/test/resources/actual/";
    private static final String GRAPHBUILD_FILE = "result.graphbuild.txt";
    private static final String PATHMERGE_FILE = "result.mergepath.txt";
    
    private static final String HADOOP_CONF_ROOT = "src/test/resources/hadoop/conf/";
    
    private MiniDFSCluster dfsCluster;

    private static JobConf conf = new JobConf();
    private int numberOfNC = 2;
    private int numPartitionPerMachine = 1;

    private Driver driver;

    @Test
    public void TestBuildGraph() throws Exception {
        copySequenceToDFS();
        buildGraph();
    }
    
//    @Test
    public void TestMergeOneIteration() throws Exception {
        copySequenceToDFS();
        buildGraph();
        MergePathsH3Driver h3 = new MergePathsH3Driver();
        h3.run(HDFS_GRAPHBUILD, HDFS_MERGED, 2, KMER_LENGTH, 1, null, conf);
        copyResultsToLocal(HDFS_MERGED, ACTUAL_ROOT + PATHMERGE_FILE, conf);
    }



    public void buildGraph() throws Exception {
        FileInputFormat.setInputPaths(conf, HDFS_SEQUENCE);
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_GRAPHBUILD));
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        driver.runJob(new GenomixJobConf(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
        copyResultsToLocal(HDFS_GRAPHBUILD, ACTUAL_ROOT + GRAPHBUILD_FILE, conf);
    }

    @Before
    public void setUp() throws Exception {
        cleanupStores();
        HyracksUtils.init();
        FileUtils.forceMkdir(new File(ACTUAL_ROOT));
        FileUtils.cleanDirectory(new File(ACTUAL_ROOT));
        startHDFS();

        conf.setInt(GenomixJobConf.KMER_LENGTH, KMER_LENGTH);
        conf.setInt(GenomixJobConf.READ_LENGTH, READ_LENGTH);
        driver = new Driver(HyracksUtils.CC_HOST,
                HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT, numPartitionPerMachine);
    }
    
    /*
     * Merge and copy a DFS directory to a local destination, converting to text if necessary. Also locally store the binary-formatted result if available.
     */
    private static void copyResultsToLocal(String hdfsSrcDir, String localDestFile, Configuration conf) throws IOException {
        String fileFormat = conf.get(GenomixJobConf.OUTPUT_FORMAT);
        if (GenomixJobConf.OUTPUT_FORMAT_TEXT.equalsIgnoreCase(fileFormat)) {
            // for text files, just concatenate them together
            FileUtil.copyMerge(FileSystem.get(conf), new Path(hdfsSrcDir),
                    FileSystem.getLocal(new Configuration()), new Path(localDestFile),
                    false, conf, null);
        } else {
            // file is binary
            // merge and store the binary format
            FileUtil.copyMerge(FileSystem.get(conf), new Path(hdfsSrcDir),
                    FileSystem.getLocal(new Configuration()), new Path(localDestFile + ".bin"),
                    false, conf, null);
            // load the Node's and write them out as text locally
            FileSystem.getLocal(new Configuration()).mkdirs(new Path(localDestFile).getParent());
            File filePathTo = new File(localDestFile);
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
            for (int i=0; i < java.lang.Integer.MAX_VALUE; i++) {
                Path path = new Path(hdfsSrcDir + "part-" + i);
                FileSystem dfs = FileSystem.get(conf);
                if (!dfs.exists(path)) {
                    break;
                }
                if (dfs.getFileStatus(path).getLen() == 0) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(dfs, path, conf);
                NodeWritable key = new NodeWritable(conf.getInt(GenomixJobConf.KMER_LENGTH, KMER_LENGTH));
                NullWritable value = NullWritable.get();
                while (reader.next(key, value)) {
                    if (key == null || value == null) {
                        break;
                    }
                    bw.write(key.toString() + "\t" + value.toString());
                    System.out.println(key.toString() + "\t" + value.toString());
                    bw.newLine();
                }
                reader.close();
            }
            bw.close();
        }

    }
    
    private boolean checkResults(String expectedPath, String actualPath, int[] poslistField) throws Exception {
        File dumped = new File(actualPath); 
        if (poslistField != null) {
            TestUtils.compareWithUnSortedPosition(new File(expectedPath), dumped, poslistField);
        } else {
            TestUtils.compareWithSortedResult(new File(expectedPath), dumped);
        }
        return true;
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    private void startHDFS() throws IOException {
        conf.addResource(new Path(HADOOP_CONF_ROOT + "core-site.xml"));
        conf.addResource(new Path(HADOOP_CONF_ROOT + "mapred-site.xml"));
        conf.addResource(new Path(HADOOP_CONF_ROOT + "hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        
        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_ROOT + "conf.xml")));
        conf.writeXml(confOutput);
        confOutput.close();
    }
    
    private void copySequenceToDFS() throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(LOCAL_SEQUENCE_FILE);
        Path dest = new Path(HDFS_SEQUENCE);
        dfs.mkdirs(dest);
        // dfs.mkdirs(result);
        dfs.copyFromLocalFile(src, dest);
    }
    
    @BeforeClass
    public static void cleanUpEntry() throws IOException {
        // local cleanup
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        if (lfs.exists(new Path(ACTUAL_ROOT))) {
            lfs.delete(new Path(ACTUAL_ROOT), true);
        }
        // dfs cleanup
        FileSystem dfs = FileSystem.get(conf);
        String[] paths = {HDFS_SEQUENCE, HDFS_GRAPHBUILD, HDFS_MERGED};
        for (String path : paths) {
            if (dfs.exists(new Path(path))) {
                dfs.delete(new Path(path), true);
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        HyracksUtils.deinit();
        cleanupHDFS();
    }

    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }
}