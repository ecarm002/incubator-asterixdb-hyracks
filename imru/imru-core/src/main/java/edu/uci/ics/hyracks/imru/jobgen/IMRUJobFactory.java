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

package edu.uci.ics.hyracks.imru.jobgen;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.dataflow.DataLoadOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.SpreadConnectorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.SpreadOD;
import edu.uci.ics.hyracks.imru.dataflow.UpdateOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.HDFSInputSplitProvider;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;

/**
 * Generates JobSpecifications for iterations of iterative map reduce
 * update, using no aggregation, a generic aggregation tree or an n-ary
 * aggregation tree. The reducers are
 * assigned to random NC's by the Hyracks scheduler.
 * 
 * @author Josh Rosen
 */
public class IMRUJobFactory implements IJobFactory {
    public static enum AGGREGATION {
        NONE,
        GENERIC,
        NARY
    };

    final ConfigurationFactory confFactory;
    List<IMRUFileSplit> inputSplits;
    String[] mapOperatorLocations;
    String[] mapNodesLocations;
    String modelNode; //on which node the model will be
    private final int fanIn;
    private final int reducerCount;
    private AGGREGATION aggType;
    UUID id = UUID.randomUUID();
    IMRUConnection imruConnection;

    public IMRUJobFactory(IMRUConnection imruConnection, String inputPaths, ConfigurationFactory confFactory,
            String type, int fanIn, int reducerCount) throws IOException, InterruptedException {
        this(imruConnection, inputPaths, confFactory, aggType(type), fanIn, reducerCount);
    }

    public static AGGREGATION aggType(String type) {
        if (type.equals("none")) {
            return AGGREGATION.NONE;
        } else if (type.equals("generic")) {
            return AGGREGATION.GENERIC;
        } else if (type.equals("nary")) {
            return AGGREGATION.NARY;
        } else {
            throw new IllegalArgumentException("Invalid aggregation tree type");
        }
    }

    /**
     * Construct a new DefaultIMRUJobFactory.
     * 
     * @param inputPaths
     *            A comma-separated list of paths specifying the input
     *            files.
     * @param confFactory
     *            A Hadoop configuration used for HDFS settings.
     * @param fanIn
     *            The number of incoming connections to each
     *            reducer (excluding the level farthest from
     *            the root).
     * @throws InterruptedException
     * @throws HyracksException
     */
    public IMRUJobFactory(IMRUConnection imruConnection, String inputPaths, ConfigurationFactory confFactory,
            AGGREGATION aggType, int fanIn, int reducerCount) throws IOException, InterruptedException {
        this.imruConnection = imruConnection;
        this.confFactory = confFactory;
        Configuration conf = confFactory == null ? null : confFactory.createConfiguration();
        HDFSInputSplitProvider inputSplitProvider = new HDFSInputSplitProvider(inputPaths, conf);
        inputSplits = inputSplitProvider.getInputSplits();
        // For repeatability of the partition assignments, seed the
        // source of
        // randomness using the job id.
        Random random = new Random(id.getLeastSignificantBits());
        mapOperatorLocations = ClusterConfig.setLocationConstraint(null, null, inputSplits, random);
        mapNodesLocations = new HashSet<String>(Arrays.asList(mapOperatorLocations)).toArray(new String[0]);
        modelNode = mapNodesLocations[0];
        this.aggType = aggType;
        this.fanIn = fanIn;
        this.reducerCount = reducerCount;
        if (aggType == AGGREGATION.NONE) {
        } else if (aggType == AGGREGATION.GENERIC) {
            if (reducerCount < 1)
                throw new IllegalArgumentException("Must specify a nonnegative aggregator count");
        } else if (aggType == AGGREGATION.NARY) {
            if (fanIn <= 1)
                throw new IllegalArgumentException("Must specify fan in greater than 1");
        }
    }

    @Override
    public UUID getId() {
        return id;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public JobSpecification generateDataLoadJob(IIMRUJobSpecification model) throws HyracksException {
        JobSpecification spec = new JobSpecification();
        IMRUOperatorDescriptor dataLoad = new DataLoadOperatorDescriptor(spec, model, inputSplits, confFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dataLoad, mapOperatorLocations);
        spec.addRoot(dataLoad);
        return spec;
    }

    public JobSpecification generateModelSpreadJob(String modelPath, int roundNum) {
        JobSpecification job = new JobSpecification();
        //        job.setFrameSize(frameSize);
        SpreadGraph graph = new SpreadGraph(mapNodesLocations, modelNode);
        //        graph.print();
        SpreadOD last = null;
        for (int i = 0; i < graph.levels.length; i++) {
            SpreadGraph.Level level = graph.levels[i];
            String[] locations = level.getLocationContraint();
            SpreadOD op = new SpreadOD(job, graph.levels, i, modelPath, imruConnection, roundNum);
            if (i > 0)
                job.connect(new SpreadConnectorDescriptor(job, graph.levels[i - 1], level), last, 0, op, 0);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, op, locations);
            last = op;
        }
        job.addRoot(last);
        return job;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public JobSpecification generateJob(IIMRUJobSpecification model, int roundNum, String modelName)
            throws HyracksException {

        JobSpecification spec = new JobSpecification();
        // Create operators
        // File reading and writing

        // IMRU Computation
        // We will have one Map operator per input file.
        IMRUOperatorDescriptor mapOperator = new MapOperatorDescriptor(spec, model, roundNum,
                "map");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, mapOperator, mapOperatorLocations);

        // Environment updating
        IMRUOperatorDescriptor updateOperator = new UpdateOperatorDescriptor(spec, model, modelName, confFactory,
                imruConnection);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, updateOperator, 1);

        if (aggType == AGGREGATION.NONE) {
            // Connect things together
            IConnectorDescriptor mapReduceConn = new MToNReplicatingConnectorDescriptor(spec);
            spec.connect(mapReduceConn, mapOperator, 0, updateOperator, 0);
        } else if (aggType == AGGREGATION.GENERIC) {
            // One level of reducers (ala Hadoop)
            IOperatorDescriptor reduceOperator = new ReduceOperatorDescriptor(spec, model, "generic reducer");
            PartitionConstraintHelper.addPartitionCountConstraint(spec, reduceOperator, reducerCount);

            // Set up the local combiners (machine-local reducers)
            IConnectorDescriptor mapReducerConn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                    OneToOneTuplePartitionComputerFactory.INSTANCE, new RangeLocalityMap(mapOperatorLocations.length));
            LocalReducerFactory.addLocalReducers(spec, mapOperator, 0, mapOperatorLocations, reduceOperator, 0,
                    mapReducerConn, model);

            // Connect things together
            IConnectorDescriptor reduceUpdateConn = new MToNReplicatingConnectorDescriptor(spec);
            spec.connect(reduceUpdateConn, reduceOperator, 0, updateOperator, 0);
        } else if (aggType == AGGREGATION.NARY) {
            // Reduce aggregation tree.
            IConnectorDescriptor reduceUpdateConn = new MToNReplicatingConnectorDescriptor(spec);
            ReduceAggregationTreeFactory.buildAggregationTree(spec, mapOperator, 0, inputSplits.size(), updateOperator,
                    0, reduceUpdateConn, fanIn, true, mapOperatorLocations, model);
        }

        spec.addRoot(updateOperator);
        return spec;
    }

}