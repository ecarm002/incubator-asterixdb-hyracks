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
package org.apache.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class LocalityAwarePartitionDataWriter implements IFrameWriter {

    private final IFrameWriter[] pWriters;
    private final IFrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;
    private final ArrayList<Integer> map;

    public LocalityAwarePartitionDataWriter(IHyracksTaskContext ctx, IPartitionWriterFactory pwFactory,
            RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc, int nConsumerPartitions,
            ILocalityMap localityMap, int senderIndex) throws HyracksDataException {
        int[] consumerPartitions = localityMap.getConsumers(senderIndex, nConsumerPartitions);
        pWriters = new IFrameWriter[consumerPartitions.length];
        appenders = new IFrameTupleAppender[consumerPartitions.length];
        for (int i = 0; i < consumerPartitions.length; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(consumerPartitions[i]);
                appenders[i] = new FrameTupleAppender();
                appenders[i].reset(new VSizeFrame(ctx), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.tpc = tpc;
        map = new ArrayList<Integer>();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            pWriters[i].open();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            if (pWriters.length == 1) {
                FrameUtils.appendToWriter(pWriters[0], appenders[0], tupleAccessor, i);
            } else {
                tpc.partition(tupleAccessor, i, pWriters.length, map);
                for (Integer p : map) {
                    FrameUtils.appendToWriter(pWriters[p], appenders[p], tupleAccessor, i);
                }
                map.clear();
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        for (int i = 0; i < appenders.length; ++i) {
            pWriters[i].fail();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            appenders[i].flush(pWriters[i], true);
            pWriters[i].close();
        }
    }

}
