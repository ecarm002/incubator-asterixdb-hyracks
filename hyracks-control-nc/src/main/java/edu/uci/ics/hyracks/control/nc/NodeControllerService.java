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
package edu.uci.ics.hyracks.control.nc;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.PartitionId;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.control.IClusterController;
import edu.uci.ics.hyracks.api.control.INodeController;
import edu.uci.ics.hyracks.api.control.NCConfig;
import edu.uci.ics.hyracks.api.control.NodeCapability;
import edu.uci.ics.hyracks.api.control.NodeParameters;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.OperatorInstanceId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.comm.ConnectionManager;
import edu.uci.ics.hyracks.control.nc.comm.DemuxDataReceiveListenerFactory;
import edu.uci.ics.hyracks.control.nc.comm.PartitionInfo;
import edu.uci.ics.hyracks.control.nc.comm.PartitionManager;
import edu.uci.ics.hyracks.control.nc.io.DeviceManager;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.runtime.OperatorRunnable;
import edu.uci.ics.hyracks.control.nc.runtime.RootHyracksContext;

public class NodeControllerService extends AbstractRemoteService implements INodeController {
    private static final long serialVersionUID = 1L;

    private NCConfig ncConfig;

    private final String id;

    private final IHyracksContext ctx;

    private final NodeCapability nodeCapability;

    private final ConnectionManager connectionManager;

    private final Timer timer;

    private IClusterController ccs;

    private Map<UUID, Joblet> jobletMap;

    private Executor executor;

    private NodeParameters nodeParameters;

    private final ServerContext serverCtx;

    private final Map<String, NCApplicationContext> applications;

    private final PartitionManager partitionManager;

    public NodeControllerService(NCConfig ncConfig) throws Exception {
        this.ncConfig = ncConfig;
        id = ncConfig.nodeId;
        DeviceManager deviceManager = new DeviceManager(null);
        IOManager ioManager = new IOManager(ncConfig.frameSize, deviceManager, 32);
        this.ctx = new RootHyracksContext(ncConfig.frameSize, ioManager);
        if (id == null) {
            throw new Exception("id not set");
        }
        nodeCapability = computeNodeCapability();
        connectionManager = new ConnectionManager(ctx, getIpAddress(ncConfig));
        jobletMap = new HashMap<UUID, Joblet>();
        executor = Executors.newCachedThreadPool();
        timer = new Timer(true);
        serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER, new File(new File(
                NodeControllerService.class.getName()), id));
        applications = new Hashtable<String, NCApplicationContext>();
        partitionManager = new PartitionManager(this);
    }

    private static Logger LOGGER = Logger.getLogger(NodeControllerService.class.getName());

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting NodeControllerService");
        connectionManager.start();
        Registry registry = LocateRegistry.getRegistry(ncConfig.ccHost, ncConfig.ccPort);
        IClusterController cc = (IClusterController) registry.lookup(IClusterController.class.getName());
        this.nodeParameters = cc.registerNode(this);

        // Schedule heartbeat generator.
        timer.schedule(new HeartbeatTask(cc), 0, nodeParameters.getHeartbeatPeriod());

        if (nodeParameters.getProfileDumpPeriod() > 0) {
            // Schedule profile dump generator.
            timer.schedule(new ProfileDumpTask(cc), 0, nodeParameters.getProfileDumpPeriod());
        }

        LOGGER.log(Level.INFO, "Started NodeControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping NodeControllerService");
        connectionManager.stop();
        LOGGER.log(Level.INFO, "Stopped NodeControllerService");
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public NodeCapability getNodeCapability() throws Exception {
        return nodeCapability;
    }

    public Map<PartitionId, PartitionInfo> getPartitionMap(UUID jobId) {
        Joblet ji = getLocalJoblet(jobId);
        if (ji == null) {
            return null;
        }
        return ji.getPartitionMap();
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    private static NodeCapability computeNodeCapability() {
        NodeCapability nc = new NodeCapability();
        nc.setCPUCount(Runtime.getRuntime().availableProcessors());
        return nc;
    }

    private static InetAddress getIpAddress(NCConfig ncConfig) throws Exception {
        String ipaddrStr = ncConfig.dataIPAddress;
        ipaddrStr = ipaddrStr.trim();
        Pattern pattern = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");
        Matcher m = pattern.matcher(ipaddrStr);
        if (!m.matches()) {
            throw new Exception(MessageFormat.format(
                    "Connection Manager IP Address String %s does is not a valid IP Address.", ipaddrStr));
        }
        byte[] ipBytes = new byte[4];
        ipBytes[0] = (byte) Integer.parseInt(m.group(1));
        ipBytes[1] = (byte) Integer.parseInt(m.group(2));
        ipBytes[2] = (byte) Integer.parseInt(m.group(3));
        ipBytes[3] = (byte) Integer.parseInt(m.group(4));
        return InetAddress.getByAddress(ipBytes);
    }

    @Override
    public void initializeJobletPhase1(String appName, final UUID jobId, byte[] planBytes, UUID stageId, int attempt,
            Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Set<Integer>> opPartitions)
            throws Exception {
        try {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet");
            }

            ApplicationContext appCtx = applications.get(appName);
            final JobPlan plan = (JobPlan) appCtx.deserialize(planBytes);

            IRecordDescriptorProvider rdp = new IRecordDescriptorProvider() {
                @Override
                public RecordDescriptor getOutputRecordDescriptor(OperatorDescriptorId opId, int outputIndex) {
                    return plan.getJobSpecification().getOperatorOutputRecordDescriptor(opId, outputIndex);
                }

                @Override
                public RecordDescriptor getInputRecordDescriptor(OperatorDescriptorId opId, int inputIndex) {
                    return plan.getJobSpecification().getOperatorInputRecordDescriptor(opId, inputIndex);
                }
            };

            final Joblet joblet = getLocalJoblet(jobId);

            final Stagelet stagelet = new Stagelet(joblet, stageId, attempt, id);
            joblet.setStagelet(stageId, stagelet);

            final JobSpecification spec = plan.getJobSpecification();

            for (ActivityNodeId hanId : tasks.keySet()) {
                IActivityNode han = plan.getActivityNodeMap().get(hanId);
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest("Initializing " + hanId + " -> " + han);
                }
                IOperatorDescriptor op = han.getOwner();
                List<IConnectorDescriptor> inputs = plan.getTaskInputs(hanId);
                List<IConnectorDescriptor> outputs = plan.getTaskOutputs(hanId);
                for (int i : tasks.get(hanId)) {
                    IOperatorNodePushable hon = han.createPushRuntime(stagelet, joblet.getEnvironment(op, i), rdp, i,
                            opPartitions.get(op.getOperatorId()).size());
                    OperatorRunnable or = new OperatorRunnable(stagelet, hon);
                    if (inputs != null) {
                        for (int j = 0; j < inputs.size(); ++j) {
                            if (j >= 1) {
                                throw new IllegalStateException();
                            }
                            IConnectorDescriptor conn = inputs.get(j);
                            OperatorDescriptorId producerOpId = plan.getJobSpecification().getProducer(conn)
                                    .getOperatorId();
                            OperatorDescriptorId consumerOpId = plan.getJobSpecification().getConsumer(conn)
                                    .getOperatorId();
                            int nProducerPartitions = opPartitions.get(producerOpId).size();
                            int nConsumerPartitions = opPartitions.get(consumerOpId).size();

                            DemuxDataReceiveListenerFactory drlf = new DemuxDataReceiveListenerFactory(stagelet, jobId,
                                    stageId);
                            IFrameReader reader = createReader(stagelet, conn, drlf, i, plan, stagelet,
                                    nProducerPartitions, nConsumerPartitions);
                            or.setFrameReader(reader);
                        }
                    }
                    if (outputs != null) {
                        for (int j = 0; j < outputs.size(); ++j) {
                            final IConnectorDescriptor conn = outputs.get(j);
                            RecordDescriptor recordDesc = spec.getConnectorRecordDescriptor(conn);
                            final OperatorDescriptorId producerOpId = plan.getJobSpecification().getProducer(conn)
                                    .getOperatorId();
                            OperatorDescriptorId consumerOpId = plan.getJobSpecification().getConsumer(conn)
                                    .getOperatorId();
                            int nProducerPartitions = opPartitions.get(producerOpId).size();
                            int nConsumerPartitions = opPartitions.get(consumerOpId).size();
                            IFrameWriter writer = conn.createSendSideWriter(stagelet, recordDesc, partitionManager, i,
                                    nProducerPartitions, nConsumerPartitions);
                            or.setFrameWriter(j, writer, recordDesc);
                        }
                    }
                    stagelet.installRunnable(new OperatorInstanceId(op.getOperatorId(), i), or);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private IFrameReader createReader(final IHyracksContext stageletContext, final IConnectorDescriptor conn,
            IConnectionDemultiplexer demux, final int receiverIndex, JobPlan plan, final Stagelet stagelet,
            int nProducerCount, int nConsumerCount) throws HyracksDataException {
        final IFrameReader reader = conn.createReceiveSideReader(stageletContext, plan.getJobSpecification()
                .getConnectorRecordDescriptor(conn), demux, receiverIndex, nProducerCount, nConsumerCount);

        return plan.getJobFlags().contains(JobFlag.PROFILE_RUNTIME) ? new IFrameReader() {
            private ICounter openCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".receiver." + receiverIndex + ".open", true);
            private ICounter closeCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".receiver." + receiverIndex + ".close", true);
            private ICounter frameCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".receiver." + receiverIndex + ".nextFrame", true);

            @Override
            public void open() throws HyracksDataException {
                reader.open();
                openCounter.update(1);
            }

            @Override
            public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
                boolean status = reader.nextFrame(buffer);
                if (status) {
                    frameCounter.update(1);
                }
                return status;
            }

            @Override
            public void close() throws HyracksDataException {
                reader.close();
                closeCounter.update(1);
            }
        } : reader;
    }

    private IFrameWriter createInterceptingWriter(final IHyracksStageletContext stageletContext,
            final IFrameWriter writer, JobPlan plan, final IConnectorDescriptor conn, final int senderIndex,
            final int receiverIndex) throws HyracksDataException {
        return plan.getJobFlags().contains(JobFlag.PROFILE_RUNTIME) ? new IFrameWriter() {
            private ICounter openCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex + ".open", true);
            private ICounter closeCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex + ".close", true);
            private ICounter frameCounter = stageletContext.getCounterContext()
                    .getCounter(
                            conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex
                                    + ".nextFrame", true);

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                openCounter.update(1);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                frameCounter.update(1);
                writer.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                closeCounter.update(1);
                writer.close();
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }
        } : writer;
    }

    private synchronized Joblet getLocalJoblet(UUID jobId) {
        Joblet ji = jobletMap.get(jobId);
        if (ji == null) {
            ji = new Joblet(this, ctx, jobId);
            jobletMap.put(jobId, ji);
        }
        return ji;
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public synchronized void cleanUpJob(UUID jobId) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleaning up after job: " + jobId);
        }
        jobletMap.remove(jobId);
        connectionManager.dumpStats();
    }

    public void notifyStageComplete(UUID jobId, UUID stageId, int attempt, Map<String, Long> stats) throws Exception {
        try {
            ccs.notifyStageletComplete(jobId, stageId, attempt, id, stats);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void notifyStageFailed(UUID jobId, UUID stageId, int attempt) throws Exception {
        try {
            ccs.notifyStageletFailure(jobId, stageId, attempt, id);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void notifyRegistration(IClusterController ccs) throws Exception {
        this.ccs = ccs;
    }

    @Override
    public NCConfig getConfiguration() throws Exception {
        return ncConfig;
    }

    private class HeartbeatTask extends TimerTask {
        private IClusterController cc;

        public HeartbeatTask(IClusterController cc) {
            this.cc = cc;
        }

        @Override
        public void run() {
            try {
                cc.nodeHeartbeat(id);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class ProfileDumpTask extends TimerTask {
        private IClusterController cc;

        public ProfileDumpTask(IClusterController cc) {
            this.cc = cc;
        }

        @Override
        public void run() {
            try {
                Map<UUID, Map<String, Long>> counterDump = new HashMap<UUID, Map<String, Long>>();
                Set<UUID> jobIds;
                synchronized (NodeControllerService.this) {
                    jobIds = new HashSet<UUID>(jobletMap.keySet());
                }
                for (UUID jobId : jobIds) {
                    Joblet ji;
                    synchronized (NodeControllerService.this) {
                        ji = jobletMap.get(jobId);
                    }
                    if (ji != null) {
                        Map<String, Long> jobletCounterDump = new HashMap<String, Long>();
                        ji.dumpProfile(jobletCounterDump);
                        counterDump.put(jobId, jobletCounterDump);
                    }
                }
                if (!counterDump.isEmpty()) {
                    cc.reportProfile(id, counterDump);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void abortJoblet(UUID jobId, UUID stageId) throws Exception {
        Joblet ji = jobletMap.get(jobId);
        if (ji != null) {
            Stagelet stagelet = ji.getStagelet(stageId);
            if (stagelet != null) {
                stagelet.abort();
                connectionManager.abortConnections(jobId, stageId);
            }
        }
    }

    @Override
    public void createApplication(String appName, boolean deployHar, byte[] serializedDistributedState)
            throws Exception {
        NCApplicationContext appCtx;
        synchronized (applications) {
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            appCtx = new NCApplicationContext(serverCtx, appName);
            applications.put(appName, appCtx);
        }
        if (deployHar) {
            HttpClient hc = new DefaultHttpClient();
            HttpGet get = new HttpGet("http://" + ncConfig.ccHost + ":"
                    + nodeParameters.getClusterControllerInfo().getWebPort() + "/applications/" + appName);
            HttpResponse response = hc.execute(get);
            InputStream is = response.getEntity().getContent();
            OutputStream os = appCtx.getHarOutputStream();
            try {
                IOUtils.copyLarge(is, os);
            } finally {
                os.close();
                is.close();
            }
        }
        appCtx.initializeClassPath();
        appCtx.setDistributedState((Serializable) appCtx.deserialize(serializedDistributedState));
        appCtx.initialize();
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        ApplicationContext appCtx = applications.remove(appName);
        if (appCtx != null) {
            appCtx.deinitialize();
        }
    }
}