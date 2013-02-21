package edu.uci.ics.hyracks.imru.data;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.file.IMRUInputSplitProvider;
import edu.uci.ics.hyracks.imru.jobgen.IMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DataSpreadDriver {
    private final static Logger LOGGER = Logger
            .getLogger(DataSpreadDriver.class.getName());
    private final IHyracksClientConnection hcc;
    private final IMRUConnection imruConnection;
    private final String app;

    File file;
    public String[] targetNodes;
    String targetPath;

    public DataSpreadDriver(IHyracksClientConnection hcc,
            IMRUConnection imruConnection, String app, File file,
            String[] targetNodes, String targetPath) {
        this.hcc = hcc;
        this.imruConnection = imruConnection;
        this.app = app;
        this.file = file;
        this.targetNodes = targetNodes;
        this.targetPath = targetPath;
    }

    public JobStatus run() throws Exception {
        Rt.p("uploading " + file.getAbsolutePath());
        imruConnection.uploadData(file.getName(), Rt.readFileByte(file));
        Rt.p("uploaded");

        JobSpecification spreadjob = IMRUJobFactory.generateModelSpreadJob(
                targetNodes, targetNodes[0], imruConnection, file.getName(), 0,
                targetPath);
        JobId spreadjobId = hcc.startJob(app, spreadjob,
                EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(spreadjobId);
        JobStatus status = hcc.getJobStatus(spreadjobId);
        long loadEnd = System.currentTimeMillis();
        return status;
    }
}
