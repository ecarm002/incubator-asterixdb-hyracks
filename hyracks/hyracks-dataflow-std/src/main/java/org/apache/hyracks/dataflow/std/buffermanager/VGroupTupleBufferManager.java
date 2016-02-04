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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This buffer manager will dived the buffers into given number of partitions.
 * The cleared partition (spilled one in the caller side) can only get no more than one frame.
 */
public class VGroupTupleBufferManager implements IPartitionedTupleBufferManager {

    private IDeallocatableFramePool framePool;
    private IFrameBufferManager[] partitionArray;
    private int[] numTuples;
    private final FixedSizeFrame appendFrame;
    private final IFrameTupleAppender appender;
    private BufferInfo tempInfo;

    public VGroupTupleBufferManager(IHyracksFrameMgrContext ctx, int partitions, int frameLimitInBytes) {
        this.framePool = new DeallocatableFramePool(ctx, frameLimitInBytes);
        this.partitionArray = new IFrameBufferManager[partitions];
        this.numTuples = new int[partitions];
        this.appendFrame = new FixedSizeFrame();
        this.appender = new FrameTupleAppender();
        this.tempInfo = new BufferInfo(null, -1, -1);
    }

    @Override
    public void reset() throws HyracksDataException {
        for (IFrameBufferManager part : partitionArray) {
            if (part != null) {
                for (int i = 0; i < part.getNumFrames(); i++) {
                    framePool.deAllocateBuffer(part.getFrame(i, tempInfo).getBuffer());
                }
                part.reset();
            }
        }
        Arrays.fill(numTuples, 0);
        appendFrame.reset(null);
    }

    @Override
    public int getNumPartitions() {
        return partitionArray.length;
    }

    @Override
    public int getNumTuples(int partition) {
        return numTuples[partition];
    }

    @Override
    public int getPhysicalSize(int partitionId) {
        int size = 0;
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                size += partition.getFrame(i, tempInfo).getLength();
            }
        }
        return size;
    }

    @Override
    public void clearPartition(int partitionId) throws HyracksDataException {
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                framePool.deAllocateBuffer(partition.getFrame(i, tempInfo).getBuffer());
            }
        }
        partitionArray[partitionId].reset();
        numTuples[partitionId] = 0;
    }

    @Override
    public boolean insertTuple(int partition, int[] fieldEndOffsets, byte[] byteArray, int start, int size,
            TuplePointer pointer) throws HyracksDataException {
        int actualSize = calculateActualSize(fieldEndOffsets, size);
        int fid = getLastBufferOrCreateNewIfNotExist(partition, actualSize);
        if (fid < 0) {
            return false;
        }
        partitionArray[partition].getFrame(fid, tempInfo);
        int tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        if (tid < 0) {
            fid = createNewBuffer(partition, actualSize);
            if (fid < 0) {
                return false;
            }
            partitionArray[partition].getFrame(fid, tempInfo);
            tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        }
        pointer.reset(makeGroupFrameId(partition, fid), tid);
        numTuples[partition]++;
        return true;
    }

    @Override
    public boolean insertTuple(int pid, IFrameTupleAccessor accessorBuild, int tid, TuplePointer tempPtr)
            throws HyracksDataException {
        return insertTuple(pid, null, accessorBuild.getBuffer().array(), accessorBuild.getTupleStartOffset(tid),
                accessorBuild.getTupleLength(tid), tempPtr);
    }

    /**
     * Spilled partition can not ask for more buffers.
     * @param partition
     * @param fieldEndOffsets
     * @param byteArray
     * @param start
     * @param size
     * @param pointer
     * @return
     * @throws HyracksDataException
     */
    @Override
    public boolean insertTupleToSpilledPartition(int partition, int[] fieldEndOffsets, byte[] byteArray, int start,
            int size, TuplePointer pointer) throws HyracksDataException {
        int actualSize = calculateActualSize(fieldEndOffsets, size);
        int fid = getLastBufferOrCreateNewIfNotExist(partition, actualSize);
        if (fid < 0) {
            return false;
        }
        partitionArray[partition].getFrame(fid, tempInfo);
        int tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        if (tid < 0) {
            return false;
        }
        pointer.reset(makeGroupFrameId(partition, fid), tid);
        numTuples[partition]++;
        return true;
    }

    @Override
    public boolean insertTupleToSpilledPartition(int pid, IFrameTupleAccessor accessorBuild, int tid,
            TuplePointer tempPtr) throws HyracksDataException {
        return insertTupleToSpilledPartition(pid, null, accessorBuild.getBuffer().array(),
                accessorBuild.getTupleStartOffset(tid), accessorBuild.getTupleLength(tid), tempPtr);
    }

    private static int calculateActualSize(int[] fieldEndOffsets, int size) {
        if (fieldEndOffsets != null) {
            return FrameHelper.calcSpaceInFrame(fieldEndOffsets.length, size);
        }
        return FrameHelper.calcSpaceInFrame(0, size);
    }

    private int makeGroupFrameId(int partition, int fid) {
        return fid * getNumPartitions() + partition;
    }

    private int parsePartitionId(int externalFrameId) {
        return externalFrameId % getNumPartitions();
    }

    private int parseFrameIdInPartition(int externalFrameId) {
        return externalFrameId / getNumPartitions();
    }

    private int createNewBuffer(int partition, int size) throws HyracksDataException {
        ByteBuffer newBuffer = requestNewBufferFromPool(size);
        if (newBuffer == null) {
            return -1;
        }
        appendFrame.reset(newBuffer);
        appender.reset(appendFrame, true);
        return partitionArray[partition].insertFrame(newBuffer);
    }

    private ByteBuffer requestNewBufferFromPool(int size) throws HyracksDataException {
        int frameSize = FrameHelper.calcAlignedFrameSizeToStore(0, size, framePool.getMinFrameSize());
        return framePool.allocateFrame(frameSize);
    }

    private int appendTupleToBuffer(BufferInfo bufferInfo, int[] fieldEndOffsets, byte[] byteArray, int start, int size)
            throws HyracksDataException {
        assert (bufferInfo.getStartOffset() == 0) : "Haven't supported yet in FrameTupleAppender";
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (fieldEndOffsets == null) {
            if (appender.append(byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        } else {
            if (appender.append(fieldEndOffsets, byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        }

        return -1;
    }

    private int getLastBufferOrCreateNewIfNotExist(int partition, int actualSize) throws HyracksDataException {
        if (partitionArray[partition] == null || partitionArray[partition].getNumFrames() == 0) {
            partitionArray[partition] = new PartitionFrameBufferManager();
            return createNewBuffer(partition, actualSize);
        }
        return partitionArray[partition].getNumFrames() - 1;
    }

    @Override
    public void close() {
        framePool.close();
        Arrays.fill(partitionArray, null);
    }

    private class PartitionFrameBufferManager implements IFrameBufferManager {

        ArrayList<ByteBuffer> buffers = new ArrayList<>();

        @Override
        public void reset() throws HyracksDataException {
            buffers.clear();
        }

        @Override
        public BufferInfo getFrame(int frameIndex, BufferInfo returnedInfo) {
            returnedInfo.reset(buffers.get(frameIndex), 0, buffers.get(frameIndex).capacity());
            return returnedInfo;
        }

        @Override
        public int getNumFrames() {
            return buffers.size();
        }

        @Override
        public int insertFrame(ByteBuffer frame) throws HyracksDataException {
            buffers.add(frame);
            return buffers.size() - 1;
        }

        @Override
        public void close() {
            buffers = null;
        }

    }

    @Override
    public ITupleBufferAccessor getTupleAccessor(final RecordDescriptor recordDescriptor) {
        return new AbstractTupleBufferAccessor() {
            FrameTupleAccessor innerAccessor = new FrameTupleAccessor(recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return innerAccessor;
            }

            @Override
            void resetInnerAccessor(TuplePointer tuplePointer) {
                partitionArray[parsePartitionId(tuplePointer.frameIndex)]
                        .getFrame(parseFrameIdInPartition(tuplePointer.frameIndex), tempInfo);
                innerAccessor.reset(tempInfo.getBuffer(), tempInfo.getStartOffset(), tempInfo.getLength());
            }
        };
    }

    @Override
    public void flushPartition(int pid, IFrameWriter writer) throws HyracksDataException {
        IFrameBufferManager partition = partitionArray[pid];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                partition.getFrame(i, tempInfo);
                tempInfo.getBuffer().position(tempInfo.getStartOffset());
                tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
                writer.nextFrame(tempInfo.getBuffer());
            }
        }

    }

}
