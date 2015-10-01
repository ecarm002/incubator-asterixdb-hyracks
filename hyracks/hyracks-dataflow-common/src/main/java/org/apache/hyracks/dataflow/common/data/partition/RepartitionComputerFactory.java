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
package org.apache.hyracks.dataflow.common.data.partition;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RepartitionComputerFactory implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;

    private int factor;
    private ITuplePartitionComputerFactory delegateFactory;
    private final ArrayList<Integer> repartitionMap;

    public RepartitionComputerFactory(int factor, ITuplePartitionComputerFactory delegate) {
        this.factor = factor;
        this.delegateFactory = delegate;
        this.repartitionMap = new ArrayList<Integer>();
    }

    @Override
    public ITuplePartitionComputer createPartitioner() {
        return new ITuplePartitionComputer() {
            private ITuplePartitionComputer delegate = delegateFactory.createPartitioner();

            @Override
            public void partition(IFrameTupleAccessor accessor, int tIndex, int nParts, List<Integer> map)
                    throws HyracksDataException {
                delegate.partition(accessor, tIndex, factor * nParts, repartitionMap);
                for (Integer h : repartitionMap) {
                    map.add(h / factor);
                }
                repartitionMap.clear();
            }
        };
    }
}