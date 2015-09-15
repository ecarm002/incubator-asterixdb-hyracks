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
package org.apache.hyracks.dataflow.std.connectors;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionReplicatorComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class PartitionReplicateDataWriter extends AbstractPartitionDataWriter {
    private final ITuplePartitionReplicatorComputer tprc;

    public PartitionReplicateDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor, ITuplePartitionReplicatorComputer tprc)
            throws HyracksDataException {
        super(ctx, consumerPartitionCount, pwFactory,recordDescriptor);
        this.tprc = tprc;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!allocatedFrame) {
            allocateFrames();
            allocatedFrame = true;
        }
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            int[] h = tprc.partition(tupleAccessor, i, consumerPartitionCount);
            for (int j = 0; j < h.length; ++j) {
                FrameUtils.appendToWriter(pWriters[h[j]], appenders[h[j]], tupleAccessor, i);
            }
        }
    }
}
