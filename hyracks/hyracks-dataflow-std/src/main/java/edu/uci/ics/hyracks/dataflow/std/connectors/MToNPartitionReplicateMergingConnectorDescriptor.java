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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.util.BitSet;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionReplicatorComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.IPartitionBatchManager;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicPartitionBatchManager;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;
import edu.uci.ics.hyracks.dataflow.std.collectors.SortMergeFrameReader;

public class MToNPartitionReplicateMergingConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    private final ITuplePartitionReplicatorComputerFactory tprcf;
    private final int[] sortFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory nkcFactory;
    private final boolean stable;

    public MToNPartitionReplicateMergingConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ITuplePartitionReplicatorComputerFactory tprcf, int[] sortFields, IBinaryComparatorFactory[] comparatorFactories,
            INormalizedKeyComputerFactory nkcFactory) {
        this(spec, tprcf, sortFields, comparatorFactories, nkcFactory, false);
    }

    public MToNPartitionReplicateMergingConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ITuplePartitionReplicatorComputerFactory tprcf, int[] sortFields, IBinaryComparatorFactory[] comparatorFactories,
            INormalizedKeyComputerFactory nkcFactory, boolean stable) {
        super(spec);
        this.tprcf = tprcf;
        this.sortFields = sortFields;
        this.comparatorFactories = comparatorFactories;
        this.nkcFactory = nkcFactory;
        this.stable = stable;
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final PartitionReplicateDataWriter rangeWriter = new PartitionReplicateDataWriter(ctx, nConsumerPartitions, edwFactory,
                recordDesc, tprcf.createPartitioner());
        return rangeWriter;
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        INormalizedKeyComputer nmkComputer = nkcFactory == null ? null : nkcFactory.createNormalizedKeyComputer();
        IPartitionBatchManager pbm = new NonDeterministicPartitionBatchManager(nProducerPartitions);
        IFrameReader sortMergeFrameReader = new SortMergeFrameReader(ctx, nProducerPartitions, nProducerPartitions,
                sortFields, comparators, nmkComputer, recordDesc, pbm);
        BitSet expectedPartitions = new BitSet();
        expectedPartitions.set(0, nProducerPartitions);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, sortMergeFrameReader, pbm);
    }
}