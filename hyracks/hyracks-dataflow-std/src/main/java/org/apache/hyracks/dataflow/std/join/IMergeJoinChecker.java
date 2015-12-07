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
package org.apache.hyracks.dataflow.std.join;

import java.io.Serializable;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IMergeJoinChecker extends Serializable {

    boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;

    boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException;

    boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException;

    boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;

    public <T extends Comparable<T>> boolean compareInterval(T start0, T end0, T start1, T end1);
}