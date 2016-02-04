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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public interface IPartitionedTupleBufferManager {

    void reset() throws HyracksDataException;

    int getNumPartitions();

    int getNumTuples(int partition);

    int getPhysicalSize(int partition);

    /**
     * Insert tuple from (int[] fieldEndOffsets, byte[] byteArray, int start, int size) into
     * specified partition. The handle is written into the tuplepointer.
     * <br>
     * If {@code byteArray} contains the {@code fieldEndOffsets} already, then please set the {@code fieldEndOffsets} as NULL
     *
     * @param partition
     *            the id of the partition to insert the tuple into
     * @param fieldEndOffsets
     *            the fieldEndOffsets which comes from the ArrayTupleBuilder, please set it to NULL if the {@code byteArray} already contains the fieldEndOffsets
     * @param byteArray
     *            the byteArray which contains the tuple
     * @param start
     *            the start offset in the {@code byteArray}
     * @param size
     *            the size of the tuple
     * @param pointer
     *            the returned pointer indicate the handle inside this buffer manager
     * @return a boolean value to indicate if the insertion succeed or not
     */
    boolean insertTuple(int partition, int[] fieldEndOffsets, byte[] byteArray, int start, int size,
            TuplePointer pointer) throws HyracksDataException;

    boolean insertTuple(int pid, IFrameTupleAccessor accessorBuild, int tid, TuplePointer tempPtr)
            throws HyracksDataException;

    /**
     * Insert the tuple into the already spilled partition, usually the spilled ones can at most occupy one frame.
     * @param partition
     * @param fieldEndOffsets
     * @param byteArray
     * @param start
     * @param size
     * @param pointer
     * @return
     * @throws HyracksDataException
     */
    boolean insertTupleToSpilledPartition(int partition, int[] fieldEndOffsets, byte[] byteArray, int start, int size,
            TuplePointer pointer) throws HyracksDataException;

    boolean insertTupleToSpilledPartition(int pid, IFrameTupleAccessor accessorBuild, int tid, TuplePointer tempPtr)
            throws HyracksDataException;

    void close();

    ITupleBufferAccessor getTupleAccessor(RecordDescriptor recordDescriptor);

    /**
     * Flush the particular partition {@code pid} to {@code writer}.
     * This partition will not be cleared.
     * Currently it is used by Join where we flush the inner partition to the join (as a frameWriter),
     * but we will still keep the inner for the next outer partition.
     * 
     * @param pid
     * @param writer
     * @throws HyracksDataException
     */
    void flushPartition(int pid, IFrameWriter writer) throws HyracksDataException;

    /**
     * Clear the particular partition.
     * 
     * @param partition
     * @throws HyracksDataException
     */
    void clearPartition(int partition) throws HyracksDataException;

}
