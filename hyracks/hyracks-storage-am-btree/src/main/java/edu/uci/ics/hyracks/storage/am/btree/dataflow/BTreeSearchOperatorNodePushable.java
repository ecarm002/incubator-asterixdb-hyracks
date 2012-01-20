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
package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private TreeIndexOpHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;

    private BTree btree;
    private boolean isForward;
    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;
    private boolean lowKeyInclusive;
    private boolean highKeyInclusive;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private MultiComparator highKeySearchCmp;
    private ITreeIndexCursor cursor;
    private ITreeIndexFrame cursorFrame;
    private BTreeOpContext opCtx;

    private RecordDescriptor recDesc;

    public BTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksStageletContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward, int[] lowKeyFields,
            int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive) {
        treeIndexOpHelper = opDesc.getTreeIndexOpHelperFactory().createTreeIndexOpHelper(opDesc, ctx, partition,
                IndexHelperOpenMode.OPEN);
        this.isForward = isForward;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor opDesc = (AbstractTreeIndexOperatorDescriptor) treeIndexOpHelper
                .getOperatorDescriptor();
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksStageletContext().getFrameSize(), recDesc);

        cursorFrame = opDesc.getTreeIndexLeafFactory().createFrame();
        cursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame);

        try {

            treeIndexOpHelper.init();
            btree = (BTree) treeIndexOpHelper.getTreeIndex();

            // construct range predicate

            int lowKeySearchFields = btree.getMultiComparator().getComparators().length;
            int highKeySearchFields = btree.getMultiComparator().getComparators().length;
            if (lowKey != null)
                lowKeySearchFields = lowKey.getFieldCount();
            if (highKey != null)
                highKeySearchFields = highKey.getFieldCount();

            IBinaryComparator[] lowKeySearchComparators = new IBinaryComparator[lowKeySearchFields];
            for (int i = 0; i < lowKeySearchFields; i++) {
                lowKeySearchComparators[i] = btree.getMultiComparator().getComparators()[i];
            }
            lowKeySearchCmp = new MultiComparator(btree.getMultiComparator().getTypeTraits(), lowKeySearchComparators);

            if (lowKeySearchFields == highKeySearchFields) {
                highKeySearchCmp = lowKeySearchCmp;
            } else {
                IBinaryComparator[] highKeySearchComparators = new IBinaryComparator[highKeySearchFields];
                for (int i = 0; i < highKeySearchFields; i++) {
                    highKeySearchComparators[i] = btree.getMultiComparator().getComparators()[i];
                }
                highKeySearchCmp = new MultiComparator(btree.getMultiComparator().getTypeTraits(),
                        highKeySearchComparators);

            }

            rangePred = new RangePredicate(isForward, null, null, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp,
                    highKeySearchCmp);

            accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksStageletContext().getFrameSize(), recDesc);

            writeBuffer = treeIndexOpHelper.getHyracksStageletContext().allocateFrame();
            tb = new ArrayTupleBuilder(btree.getMultiComparator().getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(treeIndexOpHelper.getHyracksStageletContext().getFrameSize());
            appender.reset(writeBuffer, true);

            opCtx = btree.createOpContext(IndexOp.SEARCH, treeIndexOpHelper.getLeafFrame(),
                    treeIndexOpHelper.getInteriorFrame(), null);

        } catch (Exception e) {
            treeIndexOpHelper.deinit();
        }
    }

    private void writeSearchResults() throws Exception {
        while (cursor.hasNext()) {
            tb.reset();
            cursor.next();

            ITupleReference frameTuple = cursor.getTuple();
            for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }

            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                FrameUtils.flushFrame(writeBuffer, writer);
                appender.reset(writeBuffer, true);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (lowKey != null)
                    lowKey.reset(accessor, i);
                if (highKey != null)
                    highKey.reset(accessor, i);
                rangePred.setLowKey(lowKey, lowKeyInclusive);
                rangePred.setHighKey(highKey, highKeyInclusive);

                cursor.reset();
                btree.search(cursor, rangePred, opCtx);
                writeSearchResults();
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
            try {
                cursor.close();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        } finally {
            treeIndexOpHelper.deinit();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(writeBuffer, writer);
        }
    }
}