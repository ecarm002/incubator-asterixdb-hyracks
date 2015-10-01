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

import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FieldHashPartitionComputerFamily implements ITuplePartitionComputerFamily {
    private static final long serialVersionUID = 1L;
    private final int[] hashFields;
    private final IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories;

    public FieldHashPartitionComputerFamily(int[] hashFields,
            IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories) {
        this.hashFields = hashFields;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(int seed) {
        final IBinaryHashFunction[] hashFunctions = new IBinaryHashFunction[hashFunctionGeneratorFactories.length];
        for (int i = 0; i < hashFunctionGeneratorFactories.length; ++i) {
            hashFunctions[i] = hashFunctionGeneratorFactories[i].createBinaryHashFunction(seed);
        }
        return new ITuplePartitionComputer() {
            @Override
            public void partition(IFrameTupleAccessor accessor, int tIndex, int nParts, List<Integer> map)
                    throws HyracksDataException {
                int h = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int j = 0; j < hashFields.length; ++j) {
                    int fIdx = hashFields[j];
                    IBinaryHashFunction hashFn = hashFunctions[j];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    int fh = hashFn.hash(accessor.getBuffer().array(), startOffset + slotLength + fStart,
                            fEnd - fStart);
                    h += fh;
                }
                if (h < 0) {
                    h = -(h + 1);
                }
                map.add(h % nParts);
            }
        };
    }
}
