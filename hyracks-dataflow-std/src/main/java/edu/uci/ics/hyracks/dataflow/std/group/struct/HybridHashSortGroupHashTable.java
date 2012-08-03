/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HybridHashSortGroupHashTable implements ISerializableGroupHashTable {

    protected static final int INT_SIZE = 4;
    protected static final int INIT_REF_COUNT = 8;
    protected static final int PTR_SIZE = 3;

    protected final int tableSize, framesLimit, frameSize;

    protected final ByteBuffer[] headers;
    protected final ByteBuffer[] contents;

    private final IHyracksTaskContext ctx;

    private int currentLargestFrameIndex;
    private int totalTupleCount;

    private final IAggregatorDescriptor aggregator;
    private final AggregateState aggState;

    protected final int[] keys, internalKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer tpc;

    protected final INormalizedKeyComputer firstNormalizer;

    private ByteBuffer outputBuffer;

    private LinkedList<RunFileReader> runReaders;

    private TuplePointer matchPointer;

    protected final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    private final FrameTupleAccessorForGroupHashtable compFrameAccessor1, compFrameAccessor2;

    private final FrameTupleAppenderForGroupHashtable internalAppender;

    private final FrameTupleAppender outputAppender;

    /**
     * Tuple builder for hash table insertion
     */
    private final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    /**
     * pointers for sort records in an entry
     */
    protected int[] tPointers;

    private final boolean sortInMemFlush;

    private static final Logger LOGGER = Logger.getLogger(HybridHashSortELGroupHashTable.class.getSimpleName());
    int spilledGroups = 0, insertGroups = 0, insertedRecords = 0;
    long hashComparisonCount = 0, sortComparisonCount = 0, swapCount = 0, insertTimer = 0, findMatchTimerInNS = 0,
            sortTimer = 0, actualSortTimerInNS = 0, flushHashtableToOutputTimer = 0, flushTimer = 0,
            frameFlushTimerInNS = 0;

    int usedEntries = 0;

    public HybridHashSortGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize, int[] keys,
            IBinaryComparator[] comparators, ITuplePartitionComputer tpc,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor aggregator,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc) {
        this(ctx, frameLimits, tableSize, keys, comparators, tpc, firstNormalizerComputer, aggregator, inRecDesc,
                outRecDesc, false);
    }

    public HybridHashSortGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize, int[] keys,
            IBinaryComparator[] comparators, ITuplePartitionComputer tpc,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor aggregator,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, boolean sortInMemFlush) {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.framesLimit = frameLimits;
        this.frameSize = ctx.getFrameSize();

        this.keys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.aggregator = aggregator;
        this.aggState = aggregator.createAggregateStates();

        this.tpc = tpc;
        this.comparators = comparators;
        this.firstNormalizer = firstNormalizerComputer;

        // initialize the hash table
        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        this.headers = new ByteBuffer[tableSize * INT_SIZE * 2 / frameSize + residual];

        this.outputBuffer = ctx.allocateFrame();

        this.contents = new ByteBuffer[framesLimit - 1 - headers.length];
        this.currentLargestFrameIndex = -1;
        this.totalTupleCount = 0;

        this.runReaders = new LinkedList<RunFileReader>();
        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compFrameAccessor1 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compFrameAccessor2 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.matchPointer = new TuplePointer();

        this.sortInMemFlush = sortInMemFlush;

    }

    /**
     * Reset the header page
     * 
     * @param headerFrameIndex
     */
    private void resetHeader(int headerFrameIndex) {
        for (int i = 0; i < frameSize; i += INT_SIZE) {
            headers[headerFrameIndex].putInt(i, -1);
        }
    }

    /**
     * Get the header frame index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry * 2 * INT_SIZE / frameSize;
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderTupleIndex(int entry) {
        int offset = entry * 2 * INT_SIZE % frameSize;
        return offset;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#insert(edu.uci.ics.hyracks.dataflow
     * .common.comm.io.FrameTupleAccessor, int)
     */
    @Override
    public void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {
        // FIXME
        long timer = System.currentTimeMillis();

        int entry = tpc.partition(accessor, tupleIndex, tableSize);
        if (findMatch(entry, accessor, tupleIndex)) {
            // find match; do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
            aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex, aggState);
        } else {

            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tupleIndex, aggState);
            int insertFrameIndex = -1, insertTupleIndex = -1;
            boolean inserted = false;

            if (currentLargestFrameIndex < 0) {
                currentLargestFrameIndex = 0;
            }

            if (contents[currentLargestFrameIndex] == null) {
                contents[currentLargestFrameIndex] = ctx.allocateFrame();
            }

            internalAppender.reset(contents[currentLargestFrameIndex], false);
            if (internalAppender.append(internalTupleBuilder.getFieldEndOffsets(), internalTupleBuilder.getByteArray(),
                    0, internalTupleBuilder.getSize())) {
                inserted = true;
                insertFrameIndex = currentLargestFrameIndex;
                insertTupleIndex = internalAppender.getTupleCount() - 1;
            }

            if (!inserted && currentLargestFrameIndex < contents.length - 1) {
                currentLargestFrameIndex++;
                if (contents[currentLargestFrameIndex] == null) {
                    contents[currentLargestFrameIndex] = ctx.allocateFrame();
                }
                internalAppender.reset(contents[currentLargestFrameIndex], true);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert an aggregation value.");
                } else {
                    insertFrameIndex = currentLargestFrameIndex;
                    insertTupleIndex = internalAppender.getTupleCount() - 1;
                    inserted = true;
                }
            }

            // memory is full
            if (!inserted) {
                // flush hash table and try to insert again
                flush();

                // update the match point to the header reference
                matchPointer.frameIndex = -1;
                matchPointer.tupleIndex = -1;
                // re-insert
                currentLargestFrameIndex++;
                if (contents[currentLargestFrameIndex] == null) {
                    contents[currentLargestFrameIndex] = ctx.allocateFrame();
                }
                internalAppender.reset(contents[currentLargestFrameIndex], true);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert an aggregation value.");
                } else {
                    insertFrameIndex = currentLargestFrameIndex;
                    insertTupleIndex = internalAppender.getTupleCount() - 1;
                }
            }

            // no match; new insertion
            if (matchPointer.frameIndex < 0) {
                // first record for this entry; update the header references
                int headerFrameIndex = getHeaderFrameIndex(entry);
                int headerFrameOffset = getHeaderTupleIndex(entry);
                if (headers[headerFrameIndex] == null) {
                    headers[headerFrameIndex] = ctx.allocateFrame();
                    resetHeader(headerFrameIndex);
                }
                headers[headerFrameIndex].putInt(headerFrameOffset, insertFrameIndex);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, insertTupleIndex);
                usedEntries++;
            } else {
                // update the previous reference
                hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                contents[matchPointer.frameIndex].putInt(refOffset, insertFrameIndex);
                contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE, insertTupleIndex);
            }

            insertGroups++;
            totalTupleCount++;
        }
        insertedRecords++;

        // FIXME
        insertTimer += System.currentTimeMillis() - timer;
    }

    /**
     * Flush the hash table directly to the output
     */
    public void flushHashtableToOutput(IFrameWriter outputWriter) throws HyracksDataException {

        // FIXME print hash table statistics
        printHashtableStatistics();
        long methodTimer = System.currentTimeMillis();
        long frameFlushTimer;

        if (sortInMemFlush) {
            flushEntries(outputWriter);
        } else {

            outputAppender.reset(outputBuffer, true);
            for (int i = 0; i < contents.length; i++) {
                if (contents[i] == null) {
                    continue;
                }
                hashtableRecordAccessor.reset(contents[i]);
                int tupleCount = hashtableRecordAccessor.getTupleCount();
                for (int j = 0; j < tupleCount; j++) {
                    outputTupleBuilder.reset();

                    int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(j);
                    int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                    for (int k = 0; k < internalKeys.length; k++) {
                        outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset
                                + fieldOffset + hashtableRecordAccessor.getFieldStartOffset(j, k),
                                hashtableRecordAccessor.getFieldLength(j, k));
                    }

                    aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, j, aggState);

                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        // FIXME
                        frameFlushTimer = System.nanoTime();
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        frameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to flush the hash table to the final output");
                        }
                    }
                }
            }

            if (outputAppender.getTupleCount() > 0) {
                // FIXME
                frameFlushTimer = System.nanoTime();
                FrameUtils.flushFrame(outputBuffer, outputWriter);
                frameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

                outputAppender.reset(outputBuffer, true);
            }
        }

        totalTupleCount = 0;
        usedEntries = 0;

        // FIXME
        flushHashtableToOutputTimer += System.currentTimeMillis() - methodTimer;
    }

    /**
     * Flush hash table into a run file.
     * 
     * @throws HyracksDataException
     */
    private void flush() throws HyracksDataException {

        // FIXME print hash table statistics
        printHashtableStatistics();

        long methodTimer = System.currentTimeMillis();

        FileReference runFile;
        try {
            runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                    HybridHashSortGroupHashTable.class.getSimpleName());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        RunFileWriter runWriter = new RunFileWriter(runFile, ctx.getIOManager());
        runWriter.open();
        flushEntries(runWriter);
        runWriter.close();
        runReaders.add(runWriter.createReader());
        reset();

        flushTimer += System.currentTimeMillis() - methodTimer;
    }

    private void flushEntries(IFrameWriter writer) throws HyracksDataException {
        // FIXME for debug purpose
        long frameFlushTimer;

        outputAppender.reset(outputBuffer, true);
        for (int i = 0; i < tableSize; i++) {
            long timer = System.currentTimeMillis();
            int tupleInEntry = sortEntry(i);
            sortTimer += System.currentTimeMillis() - timer;

            for (int ptr = 0; ptr < tupleInEntry; ptr++) {
                int frameIndex = tPointers[ptr * PTR_SIZE];
                int tupleIndex = tPointers[ptr * PTR_SIZE + 1];

                hashtableRecordAccessor.reset(contents[frameIndex]);
                outputTupleBuilder.reset();

                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(tupleIndex);
                int tupleLength = hashtableRecordAccessor.getTupleEndOffset(tupleIndex) - tupleOffset;
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(tupleIndex, k),
                            hashtableRecordAccessor.getFieldLength(tupleIndex, k));
                }

                aggregator.outputPartialResult(outputTupleBuilder, hashtableRecordAccessor.getBuffer().array(),
                        tupleOffset, tupleLength, hashtableRecordAccessor.getFieldCount(), INT_SIZE, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    // FIXME
                    frameFlushTimer = System.nanoTime();
                    FrameUtils.flushFrame(outputBuffer, writer);
                    frameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush an aggregation result.");
                    }
                }
                spilledGroups++;
                totalTupleCount--;
            }

            if (tupleInEntry > 0) {
                usedEntries--;
            }
        }

        if (outputAppender.getTupleCount() > 0) {
            // FIXME
            frameFlushTimer = System.nanoTime();
            FrameUtils.flushFrame(outputBuffer, writer);
            frameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

            outputAppender.reset(outputBuffer, true);
        }
    }

    protected int sortEntry(int entryID) {

        if (tPointers == null)
            tPointers = new int[INIT_REF_COUNT * PTR_SIZE];
        int ptr = 0;

        int headerFrameIndex = entryID * 2 * INT_SIZE / frameSize;
        int headerFrameOffset = entryID * 2 * INT_SIZE % frameSize;

        if (headers[headerFrameIndex] == null) {
            return 0;
        }

        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        do {
            if (entryFrameIndex < 0) {
                break;
            }
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            tPointers[ptr * PTR_SIZE] = entryFrameIndex;
            tPointers[ptr * PTR_SIZE + 1] = entryTupleIndex;
            int tStart = hashtableRecordAccessor.getTupleStartOffset(entryTupleIndex);
            int f0StartRel = hashtableRecordAccessor.getFieldStartOffset(entryTupleIndex, internalKeys[0]);
            int f0EndRel = hashtableRecordAccessor.getFieldEndOffset(entryTupleIndex, internalKeys[0]);
            int f0Start = f0StartRel + tStart + hashtableRecordAccessor.getFieldSlotsLength();
            tPointers[ptr * PTR_SIZE + 2] = firstNormalizer == null ? 0 : firstNormalizer.normalize(
                    hashtableRecordAccessor.getBuffer().array(), f0Start, f0EndRel - f0StartRel);

            ptr++;

            if (ptr * PTR_SIZE >= tPointers.length) {
                int[] newTPointers = new int[tPointers.length * 2];
                System.arraycopy(tPointers, 0, newTPointers, 0, tPointers.length);
                tPointers = newTPointers;
            }

            // move to the next record
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);

        } while (true);

        // sort records
        if (ptr > 1) {
            long timer = System.nanoTime();
            sort(0, ptr);
            actualSortTimerInNS += System.nanoTime() - timer;
        }

        return ptr;
    }

    protected void sort(int offset, int len) {
        int m = offset + (len >> 1);
        int mFrameIndex = tPointers[m * PTR_SIZE];
        int mTupleIndex = tPointers[m * PTR_SIZE + 1];
        int mNormKey = tPointers[m * PTR_SIZE + 2];
        compFrameAccessor1.reset(contents[mFrameIndex]);

        int a = offset;
        int b = a;
        int c = offset + len - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int bFrameIndex = tPointers[b * PTR_SIZE];
                int bTupleIndex = tPointers[b * PTR_SIZE + 1];
                int bNormKey = tPointers[b * PTR_SIZE + 2];
                int cmp = 0;
                if (bNormKey != mNormKey) {
                    cmp = ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    compFrameAccessor2.reset(contents[bFrameIndex]);
                    cmp = compare(compFrameAccessor2, bTupleIndex, compFrameAccessor1, mTupleIndex);
                }
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cFrameIndex = tPointers[c * PTR_SIZE];
                int cTupleIndex = tPointers[c * PTR_SIZE + 1];
                int cNormKey = tPointers[c * PTR_SIZE + 2];
                int cmp = 0;
                if (cNormKey != mNormKey) {
                    cmp = ((((long) cNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    compFrameAccessor2.reset(contents[cFrameIndex]);
                    cmp = compare(compFrameAccessor2, cTupleIndex, compFrameAccessor1, mTupleIndex);
                }
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(c, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(b++, c--);
        }

        int s;
        int n = offset + len;
        s = Math.min(a - offset, b - a);
        vecswap(offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(b, n - s, s);

        if ((s = b - a) > 1) {
            sort(offset, s);
        }
        if ((s = d - c) > 1) {
            sort(n - s, s);
        }
    }

    private void swap(int a, int b) {
        swapCount++;
        for (int i = 0; i < PTR_SIZE; i++) {
            int t = tPointers[a * PTR_SIZE + i];
            tPointers[a * PTR_SIZE + i] = tPointers[b * PTR_SIZE + i];
            tPointers[b * PTR_SIZE + i] = t;
        }
    }

    private void vecswap(int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(a, b);
        }
    }

    private boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) {
        // FIXME
        long methodTimer = System.nanoTime();

        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            // FIXME
            findMatchTimerInNS += System.nanoTime() - methodTimer;
            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            matchPointer.frameIndex = entryFrameIndex;
            matchPointer.tupleIndex = entryTupleIndex;
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            if (compare(accessor, tupleIndex, hashtableRecordAccessor, entryTupleIndex) == 0) {
                // FIXME
                findMatchTimerInNS += System.nanoTime() - methodTimer;
                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }

        // FIXME
        findMatchTimerInNS += System.nanoTime() - methodTimer;
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#getRunFileReaders()
     */
    @Override
    public LinkedList<RunFileReader> getRunFileReaders() {
        return runReaders;
    }

    private int compare(FrameTupleAccessor accessor, int tupleIndex, FrameTupleAccessorForGroupHashtable hashAccessor,
            int hashTupleIndex) {
        hashComparisonCount++;
        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int tStart1 = hashAccessor.getTupleStartOffset(hashTupleIndex);
        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < keys.length; ++i) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, keys[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, keys[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = hashAccessor.getFieldStartOffset(hashTupleIndex, internalKeys[i]);
            int fEnd1 = hashAccessor.getFieldEndOffset(hashTupleIndex, internalKeys[i]);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, hashAccessor
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int compare(FrameTupleAccessorForGroupHashtable accessor1, int tupleIndex1,
            FrameTupleAccessorForGroupHashtable accessor2, int tupleIndex2) {
        sortComparisonCount++;
        int tStart1 = accessor1.getTupleStartOffset(tupleIndex1);
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        int tStart2 = accessor2.getTupleStartOffset(tupleIndex2);
        int fStartOffset2 = accessor2.getFieldSlotsLength() + tStart2;

        for (int i = 0; i < internalKeys.length; ++i) {
            int fStart1 = accessor1.getFieldStartOffset(tupleIndex1, internalKeys[i]);
            int fEnd1 = accessor1.getFieldEndOffset(tupleIndex1, internalKeys[i]);
            int fLen1 = fEnd1 - fStart1;

            int fStart2 = accessor2.getFieldStartOffset(tupleIndex2, internalKeys[i]);
            int fEnd2 = accessor2.getFieldEndOffset(tupleIndex2, internalKeys[i]);
            int fLen2 = fEnd2 - fStart2;

            int c = comparators[i].compare(accessor1.getBuffer().array(), fStart1 + fStartOffset1, fLen1, accessor2
                    .getBuffer().array(), fStart2 + fStartOffset2, fLen2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public void reset() {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                resetHeader(i);
            }
        }
        for (int i = 0; i < contents.length; i++) {
            if (contents[i] != null) {
                contents[i].putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            }
        }

        usedEntries = 0;
        totalTupleCount = 0;
        insertedRecords = 0;
        currentLargestFrameIndex = -1;
    }

    @Override
    public void finishup() throws HyracksDataException {
        if (runReaders.size() > 0) {
            flush();
        }
    }

    /**
     * Close the hash table. Note that only memory allocated by frames are freed. Aggregation
     * states maintained in {@link #aggState} and run file readers in {@link #runReaders} should
     * be valid for later processing.
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < headers.length; i++) {
            headers[i] = null;
        }
        for (int i = 0; i < contents.length; i++) {
            contents[i] = null;
        }
        outputBuffer = null;
        LOGGER.warning("HybridHashSortGroupHashTable-Close\t" + insertGroups + "\t" + spilledGroups + "\t"
                + headers.length + "\t" + hashComparisonCount + "\t" + sortComparisonCount + "\t" + swapCount + "\t"
                + runReaders.size() + "\t" + insertTimer + "\t" + findMatchTimerInNS + "\t" + sortTimer + "\t"
                + actualSortTimerInNS + "\t" + flushHashtableToOutputTimer + "\t" + flushTimer + "\t"
                + frameFlushTimerInNS);
    }

    @Override
    public int getTupleCount() {
        return totalTupleCount;
    }

    @Override
    public int getFrameSize() {
        return headers.length + contents.length + 1;
    }

    private void printHashtableStatistics() {
        LOGGER.warning("HybridHashGroupHashTable-HashtableStatistics\t" + tableSize + "\t" + usedEntries + "\t"
                + totalTupleCount + "\t" + insertedRecords);
        if (usedEntries != 0 && totalTupleCount / usedEntries >= 2) {
            // print the hash table distribution
            long startTimers = System.currentTimeMillis();
            StringBuilder sbder = new StringBuilder();
            for (int i = 0; i < tableSize; i++) {
                int cnt = getEntryLength(i);
                if (cnt > 0)
                    sbder.append(i).append(" ").append(cnt).append("\n");
            }
            LOGGER.warning("HashtableHistogram\t" + (System.currentTimeMillis() - startTimers) + "\n"
                    + sbder.toString());
        }
    }

    private int getEntryLength(int entry) {

        int cnt = 0;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            return cnt;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
            cnt++;
        }

        return cnt;
    }

}