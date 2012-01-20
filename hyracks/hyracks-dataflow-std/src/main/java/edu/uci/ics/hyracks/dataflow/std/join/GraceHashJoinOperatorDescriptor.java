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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RepartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class GraceHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String RELATION0 = "Rel0";
    private static final String RELATION1 = "Rel1";

    private static final long serialVersionUID = 1L;
    private final int[] keys0;
    private final int[] keys1;
    private final int inputsize0;
    private final int recordsPerFrame;
    private final int memsize;
    private final double factor;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;

    public GraceHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.recordsPerFrame = recordsPerFrame;
        this.factor = factor;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.isLeftOuter = false;
        this.nullWriterFactories1 = null;
        recordDescriptors[0] = recordDescriptor;
    }

    public GraceHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, boolean isLeftOuter,
            INullWriterFactory[] nullWriterFactories1) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.recordsPerFrame = recordsPerFrame;
        this.factor = factor;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        HashPartitionActivityNode part0 = new HashPartitionActivityNode(RELATION0, keys0, 0);
        HashPartitionActivityNode part1 = new HashPartitionActivityNode(RELATION1, keys1, 1);
        JoinActivityNode join = new JoinActivityNode();

        builder.addTask(part0);
        builder.addSourceEdge(0, part0, 0);

        builder.addTask(part1);
        builder.addSourceEdge(1, part1, 0);

        builder.addTask(join);
        builder.addBlockingEdge(part0, part1);
        builder.addBlockingEdge(part1, join);

        builder.addTargetEdge(0, join, 0);
    }

    public int getMemorySize() {
        return memsize;
    }

    private class HashPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private String partitionsKey;
        private int operatorInputIndex;
        private int keys[];

        public HashPartitionActivityNode(String partitionsKey, int keys[], int operatorInputIndex) {
            this.partitionsKey = partitionsKey;
            this.keys = keys;
            this.operatorInputIndex = operatorInputIndex;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksStageletContext ctx,
                final IOperatorEnvironment env, final IRecordDescriptorProvider recordDescProvider, int partition,
                final int nPartitions) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private final FrameTupleAccessor accessor0 = new FrameTupleAccessor(ctx.getFrameSize(),
                        recordDescProvider.getInputRecordDescriptor(getOperatorId(), operatorInputIndex));

                private final ITuplePartitionComputer hpc = new FieldHashPartitionComputerFactory(keys,
                        hashFunctionFactories).createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private ByteBuffer[] outbufs;
                private RunFileWriter[] fWriters;
                private final int numPartitions = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));

                @Override
                public void close() throws HyracksDataException {
                    for (int i = 0; i < numPartitions; i++) {
                        ByteBuffer head = outbufs[i];
                        accessor0.reset(head);
                        if (accessor0.getTupleCount() > 0) {
                            write(i, head);
                        }
                        closeWriter(i);
                    }

                    env.set(partitionsKey, fWriters);
                }

                private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = fWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = fWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.getJobletContext().createWorkspaceFile(partitionsKey);
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        fWriters[i] = writer;
                    }
                    writer.nextFrame(head);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor0.reset(buffer);
                    int tCount = accessor0.getTupleCount();
                    for (int i = 0; i < tCount; ++i) {

                        int entry = hpc.partition(accessor0, i, numPartitions);
                        ByteBuffer outbuf = outbufs[entry];
                        appender.reset(outbuf, true);
                        while (true) {
                            if (appender.append(accessor0, i)) {
                                break;
                            } else {
                                // buffer is full, ie. we cannot fit the tuple
                                // into the buffer -- write it to disk
                                write(entry, outbuf);
                                outbuf.clear();
                                appender.reset(outbuf, true);
                            }
                        }
                    }
                }

                @Override
                public void open() throws HyracksDataException {
                    outbufs = new ByteBuffer[numPartitions];
                    fWriters = new RunFileWriter[numPartitions];
                    for (int i = 0; i < numPartitions; i++) {
                        outbufs[i] = ctx.allocateFrame();
                    }
                }

                @Override
                public void flush() throws HyracksDataException {
                }
            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return GraceHashJoinOperatorDescriptor.this;
        }
    }

    private class JoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksStageletContext ctx,
                final IOperatorEnvironment env, final IRecordDescriptorProvider recordDescProvider, int partition,
                final int nPartitions) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
                }
            }

            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                private InMemoryHashJoin joiner;

                private RunFileWriter[] buildWriters;
                private RunFileWriter[] probeWriters;
                private final int numPartitions = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));

                @Override
                public void initialize() throws HyracksDataException {
                    buildWriters = (RunFileWriter[]) env.get(RELATION1);
                    probeWriters = (RunFileWriter[]) env.get(RELATION0);

                    ITuplePartitionComputer hpcRep0 = new RepartitionComputerFactory(numPartitions,
                            new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)).createPartitioner();
                    ITuplePartitionComputer hpcRep1 = new RepartitionComputerFactory(numPartitions,
                            new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)).createPartitioner();

                    writer.open();// open for probe

                    ByteBuffer buffer = ctx.allocateFrame();// input
                    // buffer
                    int tableSize = (int) (numPartitions * recordsPerFrame * factor);
                    for (int partitionid = 0; partitionid < numPartitions; partitionid++) {
                        RunFileWriter buildWriter = buildWriters[partitionid];
                        RunFileWriter probeWriter = probeWriters[partitionid];
                        if ((buildWriter == null && !isLeftOuter) || probeWriter == null) {
                            continue;
                        }
                        joiner = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                                hpcRep0, new FrameTupleAccessor(ctx.getFrameSize(), rd1), hpcRep1,
                                new FrameTuplePairComparator(keys0, keys1, comparators), isLeftOuter, nullWriters1);

                        // build
                        if (buildWriter != null) {
                            RunFileReader buildReader = buildWriter.createReader();
                            buildReader.open();
                            while (buildReader.nextFrame(buffer)) {
                                ByteBuffer copyBuffer = ctx.allocateFrame();
                                FrameUtils.copy(buffer, copyBuffer);
                                joiner.build(copyBuffer);
                                buffer.clear();
                            }
                            buildReader.close();
                        }

                        // probe
                        RunFileReader probeReader = probeWriter.createReader();
                        probeReader.open();
                        while (probeReader.nextFrame(buffer)) {
                            joiner.join(buffer, writer);
                            buffer.clear();
                        }
                        probeReader.close();
                        joiner.closeJoin(writer);
                    }
                    writer.close();
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    env.set(RELATION1, null);
                    env.set(RELATION0, null);
                }
            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return GraceHashJoinOperatorDescriptor.this;
        }
    }
}