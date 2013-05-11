/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.genomix.hyracks.data.accessors;

import edu.uci.ics.genomix.hyracks.data.primitive.KmerPointable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class KmerBinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
    private static final long serialVersionUID = 1L;

    @Override
    public IBinaryHashFunction createBinaryHashFunction(final int seed) {

        return new IBinaryHashFunction() {
            private KmerPointable p = new KmerPointable();

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                if (length + offset >= bytes.length)
                    throw new IllegalStateException("out of bound");
                p.set(bytes, offset, length);
                int hash = p.hash() * (seed + 1);
                if (hash < 0) {
                    hash = -(hash + 1);
                }
                return hash;
            }
        };
    }
}