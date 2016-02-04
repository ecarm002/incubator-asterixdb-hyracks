/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.group;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleBufferAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VGroupTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class HashSpillableTableFactory implements ISpillableTableFactory {

    private static Logger LOGGER = Logger.getLogger(HashSpillableTableFactory.class.getName());
    private static final int DEFAULT_TUPLE_PER_FRAME = 10;
    private static double factor = 1.1;
    private static final long serialVersionUID = 1L;
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;

    public HashSpillableTableFactory(IBinaryHashFunctionFamily[] hashFunctionFamilies) {
        this.hashFunctionFamilies = hashFunctionFamilies;
    }

    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, int suggestTableSize, long fileSize,
            final int[] keyFields, final IBinaryComparator[] comparators,
            final INormalizedKeyComputer firstKeyNormalizerFactory, IAggregatorDescriptorFactory aggregateFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, final int framesLimit,
            final int seed) throws HyracksDataException {
        if (framesLimit < 2) {
            throw new HyracksDataException("The frame limit is too small to partition the data");
        }
        final int tableSize = Math.max(suggestTableSize, framesLimit * DEFAULT_TUPLE_PER_FRAME);

        final int[] aggregatedKeys = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            aggregatedKeys[i] = i;
        }

        final FrameTuplePairComparator ftpcInputVSAggregate = new FrameTuplePairComparator(keyFields, aggregatedKeys,
                comparators);

        final ITuplePartitionComputer tpc = new FieldHashPartitionComputerFamily(keyFields, hashFunctionFamilies)
                .createPartitioner(seed);

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, aggregatedKeys, null);

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final ArrayTupleBuilder stateTupleBuilder;
        if (keyFields.length < outRecordDescriptor.getFields().length) {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        } else {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length + 1);
        }

        //TODO(jf) research on the optimized partition size
        final int numPhysicalPartitions = getNumOfPartitions(tableSize,
                fileSize <= 0 ? -1 : (int) (fileSize / ctx.getInitialFrameSize()), framesLimit - 1);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("create hashtable, table size:" + tableSize + " file size:" + fileSize + " physical partitions:"
                    + numPhysicalPartitions);
        }

        final ArrayTupleBuilder outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        final IPartitionedTupleBufferManager bufferManager = new VGroupTupleBufferManager(ctx, numPhysicalPartitions,
                framesLimit * ctx.getInitialFrameSize());

        final ISerializableTable metaTable = new SerializableHashTable(tableSize, ctx);

        final ITupleBufferAccessor bufferAccessor = bufferManager.getTupleAccessor(outRecordDescriptor);

        return new ISpillableTable() {

            private TuplePointer pointer = new TuplePointer();
            private BitSet flushedSet = new BitSet(numPhysicalPartitions);

            private FrameTupleAppender outputAppender = new FrameTupleAppender(new VSizeFrame(ctx));

            @Override
            public void close() throws HyracksDataException {
                metaTable.close();
                aggregator.close();
            }

            @Override
            public void clear(int physicalPartition) throws HyracksDataException {
                for (int p = getFirstLogicalPartition(physicalPartition); p >= 0
                        && p < tableSize; p = getNextLogicalPartition(p)) {
                    metaTable.delete(p);
                }
                bufferManager.clearPartition(physicalPartition);
            }

            private int getPhysicalPartition(int logicalPartition) {
                return logicalPartition % numPhysicalPartitions;
            }

            private int getFirstLogicalPartition(int physicalPartition) {
                return physicalPartition;
            }

            private int getNextLogicalPartition(int curLogicalPartition) {
                assert curLogicalPartition >= 0;
                return curLogicalPartition + numPhysicalPartitions;
            }

            @Override
            public boolean insert(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int logicalPartition = tpc.partition(accessor, tIndex, tableSize);
                for (int i = 0; i < metaTable.getTupleCount(logicalPartition); i++) {
                    metaTable.getTuplePointer(logicalPartition, i, pointer);
                    bufferAccessor.reset(pointer);
                    int c = ftpcInputVSAggregate.compare(accessor, tIndex, bufferAccessor);
                    if (c == 0) {
                        aggregateExistingTuple(accessor, tIndex, bufferAccessor, pointer.tupleIndex);
                        return true;
                    }
                }

                return insertNewAggregateEntry(logicalPartition, accessor, tIndex);
            }

            private boolean insertNewAggregateEntry(int logicalPartition, IFrameTupleAccessor accessor, int tIndex)
                    throws HyracksDataException {
                initStateTupleBuilder(accessor, tIndex);
                int physicalPid = getPhysicalPartition(logicalPartition);
                if (flushedSet.get(physicalPid)) {
                    if (!bufferManager.insertTupleToSpilledPartition(physicalPid,
                            stateTupleBuilder.getFieldEndOffsets(), stateTupleBuilder.getByteArray(), 0,
                            stateTupleBuilder.getSize(), pointer)) {
                        return false;
                    }
                } else {
                    if (!bufferManager.insertTuple(physicalPid, stateTupleBuilder.getFieldEndOffsets(),
                            stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize(), pointer)) {
                        return false;
                    }
                }
                metaTable.insert(logicalPartition, pointer);
                return true;
            }

            private void initStateTupleBuilder(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                stateTupleBuilder.reset();
                for (int k = 0; k < keyFields.length; k++) {
                    stateTupleBuilder.addField(accessor, tIndex, keyFields[k]);
                }
                aggregator.init(stateTupleBuilder, accessor, tIndex, aggregateState);
            }

            private void aggregateExistingTuple(IFrameTupleAccessor accessor, int tIndex,
                    ITupleBufferAccessor bufferAccessor, int tupleIndex) throws HyracksDataException {
                aggregator.aggregate(accessor, tIndex, bufferAccessor, tupleIndex, aggregateState);
            }

            @Override
            public int flushFrames(int physicalPartition, IFrameWriter writer, AggregateType type)
                    throws HyracksDataException {
                int count = 0;
                for (int logicalPid = getFirstLogicalPartition(physicalPartition); logicalPid >= 0
                        && logicalPid < tableSize; logicalPid = getNextLogicalPartition(logicalPid)) {
                    count += metaTable.getTupleCount(logicalPid);
                    for (int tid = 0; tid < metaTable.getTupleCount(logicalPid); tid++) {
                        metaTable.getTuplePointer(logicalPid, tid, pointer);
                        bufferAccessor.reset(pointer);
                        outputTupleBuilder.reset();
                        for (int k = 0; k < aggregatedKeys.length; k++) {
                            outputTupleBuilder.addField(bufferAccessor.getBuffer().array(),
                                    bufferAccessor.getAbsFieldStartOffset(aggregatedKeys[k]),
                                    bufferAccessor.getFieldLength(aggregatedKeys[k]));
                        }

                        switch (type) {
                            case PARTIAL:
                                aggregator.outputPartialResult(outputTupleBuilder, bufferAccessor, pointer.tupleIndex,
                                        aggregateState);
                                break;
                            case FINAL:
                                aggregator.outputFinalResult(outputTupleBuilder, bufferAccessor, pointer.tupleIndex,
                                        aggregateState);
                                break;
                        }

                        if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            outputAppender.write(writer, true);
                            if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                    outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                throw new HyracksDataException("The output item is too large to be fit into a frame.");
                            }
                        }
                    }
                }
                outputAppender.write(writer, true);
                flushedSet.set(physicalPartition);
                return count;
            }

            @Override
            public int getNumPartitions() {
                return bufferManager.getNumPartitions();
            }

            @Override
            public int findVictimPartition(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int logicalPartition = tpc.partition(accessor, tIndex, tableSize);
                int physicalPartition = getPhysicalPartition(logicalPartition);
                if (bufferManager.getNumTuples(physicalPartition) > 0) {
                    return physicalPartition;
                }
                int flushedPartition = findMaxSizeIdInFlushedPartitions();
                if (flushedPartition < 0) {
                    return findMaxSizeIdInMemPartitions();
                }
                // if this maxSize partition is too small to flush, we need to find a better candidate
                if (bufferManager.getPhysicalSize(flushedPartition) <= ctx.getInitialFrameSize()) {
                    int max = findMaxSizeIdInMemPartitions();
                    if (bufferManager.getPhysicalSize(max) > ctx.getInitialFrameSize()) {
                        flushedPartition = max;
                    }

                }
                return flushedPartition;
            }

            private int findMaxSizeIdInFlushedPartitions() {
                int maxSize = 0;
                int maxId = -1;
                for (int i = flushedSet.nextSetBit(0); i >= 0
                        && i < bufferManager.getNumPartitions(); i = flushedSet.nextSetBit(i + 1)) {
                    int size = bufferManager.getPhysicalSize(i);
                    if (maxSize < size) {
                        maxSize = size;
                        maxId = i;
                    }
                }
                return maxId;
            }

            private int findMaxSizeIdInMemPartitions() {
                int maxSize = 0;
                int maxId = -1;
                for (int i = flushedSet.nextClearBit(0); i >= 0
                        && i < bufferManager.getNumPartitions(); i = flushedSet.nextClearBit(i + 1)) {
                    int size = bufferManager.getPhysicalSize(i);
                    if (maxSize < size) {
                        maxSize = size;
                        maxId = i;
                    }
                }
                return maxId;
            }

        };
    }

    private int getNumOfPartitions(int tableSize, int fileSizeInFrames, int memSize) {
        if (fileSizeInFrames < 0) { // unknown size
            fileSizeInFrames = tableSize / DEFAULT_TUPLE_PER_FRAME;
        }
        int numberOfPartitions = 0;
        if (memSize > fileSizeInFrames) {
            return 1; // all in memory, we will create a big partition
        }
        numberOfPartitions = (int) (Math.ceil((fileSizeInFrames * factor - memSize) / (memSize - 1)));
        if (numberOfPartitions <= 0) {
            numberOfPartitions = 1; //becomes in-memory hash join
        }
        if (numberOfPartitions > memSize) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(fileSizeInFrames * factor));
            return Math.max(2, Math.min(numberOfPartitions, memSize));
        }
        return numberOfPartitions;
    }

}
