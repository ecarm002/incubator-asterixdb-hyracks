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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class InMemoryHashGroupJoin {
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private final FrameTupleAppender appender;
    private final IBinaryComparatorFactory[] comparator;
    private final ByteBuffer outBuffer;
	private final GroupJoinHelper gByTable;

    public InMemoryHashGroupJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessor0,
            FrameTupleAccessor accessor1, IBinaryComparatorFactory[] groupComparator, ITuplePartitionComputerFactory gByTpc0,
            ITuplePartitionComputerFactory gByTpc1, RecordDescriptor gByInRecordDescriptor, RecordDescriptor gByOutRecordDescriptor,
            IAggregatorDescriptorFactory aggregatorFactory, int[] joinAttributes, int[] groupAttributes, 
            int[] decorAttributes, boolean isLeftOuter, INullWriter[] nullWriters1) throws HyracksDataException {
        buffers = new ArrayList<ByteBuffer>();
        this.accessorBuild = accessor0;
        this.accessorProbe = accessor1;
        appender = new FrameTupleAppender(ctx.getFrameSize());
        this.comparator = groupComparator;
        
        outBuffer = ctx.allocateFrame();
        appender.reset(outBuffer, true);

        gByTable = new GroupJoinHelper(ctx, groupAttributes, joinAttributes, decorAttributes, comparator, gByTpc0, 
        		gByTpc1, aggregatorFactory, gByInRecordDescriptor, gByOutRecordDescriptor, isLeftOuter, nullWriters1, tableSize);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException, IOException {
        buffers.add(buffer);
        gByTable.build(accessorBuild, buffer);
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        gByTable.insert(accessorProbe, buffer);
    }

    public void write(IFrameWriter writer) throws HyracksDataException {
    	gByTable.write(writer);
    }
    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            flushFrame(outBuffer, writer);
        }
        gByTable.close();
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        writer.nextFrame(buffer);
        buffer.position(0);
        buffer.limit(buffer.capacity());
    }
}