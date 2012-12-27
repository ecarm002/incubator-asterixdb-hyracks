package edu.uci.ics.hyracks.imru.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.EnumSet;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.base.IJobFactory;

/**
 * Schedules iterative map reduce update jobs.
 *
 * @param <Model>
 *            The class used to represent the global model that is
 *            persisted between iterations.
 * @author Josh Rosen
 */
public class IMRUDriver<Model extends IModel> {
    private final static Logger LOGGER = Logger.getLogger(IMRUDriver.class.getName());
    private final IIMRUJobSpecification<Model> imruSpec;
    private Model model;
    private final IHyracksClientConnection hcc;
    private final IJobFactory jobFactory;
    private final Configuration conf;
    private final String tempPath;
    private final String app;
    private final UUID id;

    private int iterationCount;

    /**
     * Construct a new IMRUDriver.
     *
     * @param hcc
     *            A client connection to the cluster controller.
     * @param imruSpec
     *            The IIMRUJobSpecification to use.
     * @param initialModel
     *            The initial global model.
     * @param jobFactory
     *            A factory for constructing Hyracks dataflows for
     *            this IMRU job.
     * @param conf
     *            A Hadoop configuration used for HDFS settings.
     * @param tempPath
     *            The DFS directory to write temporary files to.
     * @param app
     *            The application name to use when running the jobs.
     */
    public IMRUDriver(IHyracksClientConnection hcc, IIMRUJobSpecification<Model> imruSpec, Model initialModel,
            IJobFactory jobFactory, Configuration conf, String tempPath, String app) {
        this.imruSpec = imruSpec;
        this.model = initialModel;
        this.hcc = hcc;
        this.jobFactory = jobFactory;
        this.conf = conf;
        this.tempPath = tempPath;
        this.app = app;
        id = UUID.randomUUID();
        iterationCount = 0;
    }

    /**
     * Run iterative map reduce update.
     *
     * @return The JobStatus of the IMRU computation.
     * @throws Exception
     */
    public JobStatus run() throws Exception {
        LOGGER.info("Starting IMRU job with driver id " + id);
        iterationCount = 0;
        // The path containing the model to be used as input for a
        // round.
        Path modelInPath;
        // The path containing the updated model written by the
        // Update operator.
        Path modelOutPath;
        // For the first round, the initial model is written by the
        // driver.
        writeModelToFile(model, new Path(tempPath, "IMRU-" + id + "-iter" + 0));

        // Data load
        LOGGER.info("Starting data load");
        long loadStart = System.currentTimeMillis();
        JobStatus status = runDataLoad();
        long loadEnd = System.currentTimeMillis();
        LOGGER.info("Finished data load in " + (loadEnd - loadStart) + " milliseconds");
        if (status == JobStatus.FAILURE) {
            LOGGER.severe("Failed during data load");
            return JobStatus.FAILURE;
        }

        // Iterations
        do {
            iterationCount++;
            modelOutPath = new Path(tempPath, "IMRU-" + id + "-iter" + iterationCount);
            modelInPath = new Path(tempPath, "IMRU-" + id + "-iter" + (iterationCount - 1));

            LOGGER.info("Starting round " + iterationCount);
            long start = System.currentTimeMillis();
            status = runIMRUIteration(modelInPath.toString(), modelOutPath.toString(), iterationCount);
            long end = System.currentTimeMillis();
            LOGGER.info("Finished round " + iterationCount + " in " + (end - start) + " milliseconds");

            if (status == JobStatus.FAILURE) {
                LOGGER.severe("Failed during iteration " + iterationCount);
                return JobStatus.FAILURE;
            }
            model = readModelFromFile(modelOutPath);
            // TODO: clean up temporary files
        } while (!imruSpec.shouldTerminate(model));
        return JobStatus.TERMINATED;
    }

    /**
     * @return The number of iterations performed.
     */
    public int getIterationCount() {
        return iterationCount;
    }

    /**
     * @return The most recent global model.
     */
    public Model getModel() {
        return model;
    }

    /**
     * @return The IMRU job ID.
     */
    public UUID getId() {
        return id;
    }

    /**
     * Run the dataflow to cache the input records.

     * @return The JobStatus of the job after completion or failure.
     * @throws Exception
     */
    private JobStatus runDataLoad() throws Exception {
        JobSpecification job = jobFactory.generateDataLoadJob(imruSpec, id);
        JobId jobId = hcc.startJob(app, job, EnumSet
                .of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
//        JobId jobId = hcc.createJob(app, job);
//        hcc.start(jobId);
//        hcc.waitForCompletion(jobId);
        return hcc.getJobStatus(jobId);
    }

    /**
     * Run the dataflow for a single IMRU iteration.
     *
     * @param envInPath
     *            The HDFS path to read the current environment from.
     * @param envOutPath
     *            The DFS path to write the updated environment to.
     * @param iterationNum
     *            The iteration number.
     * @return The JobStatus of the job after completion or failure.
     * @throws Exception
     */
    private JobStatus runIMRUIteration(String envInPath, String envOutPath, int iterationNum) throws Exception {
        JobSpecification job = jobFactory.generateJob(imruSpec, id, iterationNum, envInPath, envOutPath);
        JobId jobId = hcc.startJob(app, job, EnumSet
                .of(JobFlag.PROFILE_RUNTIME));
//        JobId jobId = hcc.createJob(app, job);
//        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        return hcc.getJobStatus(jobId);
    }

    /**
     * Read the updated model from a file.
     *
     * @param modelPath
     *            The DFS file containing the updated model.
     * @return The updated model.
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Model readModelFromFile(Path modelPath) throws IOException, ClassNotFoundException {
        // Deserialize the environment so it can be passed to
        // shouldTerminate().
        FileSystem dfs = FileSystem.get(conf);
        FSDataInputStream fileInput = dfs.open(modelPath);
        ObjectInputStream input = new ObjectInputStream(fileInput);
        @SuppressWarnings("unchecked")
        Model newModel = (Model) input.readObject();
        input.close();
        return newModel;
    }

    /**
     * Write the model to a file.
     *
     * @param model
     *            The model to write.
     * @param modelPath
     *            The DFS file to write the updated model to.
     * @throws IOException
     */
    private void writeModelToFile(IModel model, Path modelPath) throws IOException {
        // Serialize the model so it can be read during the next
        // iteration.
        FileSystem dfs = FileSystem.get(conf);
        FSDataOutputStream fileOutput = dfs.create(modelPath, true);
        ObjectOutputStream output = new ObjectOutputStream(fileOutput);
        output.writeObject(model);
        output.close();
    }

}