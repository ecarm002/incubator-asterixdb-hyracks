package edu.uci.ics.hyracks.storage.am.btree;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.ITreeIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

@SuppressWarnings("rawtypes")
public class OrderedIndexTestUtils extends TreeIndexTestUtils {
    private static final Logger LOGGER = Logger.getLogger(OrderedIndexTestUtils.class.getName());

    private static void compareActualAndExpected(ITupleReference actual, CheckTuple expected,
            ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        for (int i = 0; i < fieldSerdes.length; i++) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(actual.getFieldData(i), actual.getFieldStart(i),
                    actual.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object actualObj = fieldSerdes[i].deserialize(dataIn);
            if (!actualObj.equals(expected.get(i))) {
                fail("Actual and expected fields do not match on field " + i + ".\nExpected: " + expected.get(i)
                        + "\nActual  : " + actualObj);
            }
        }
    }

    @SuppressWarnings("unchecked")
    // Create a new TreeSet containing the elements satisfying the prefix
    // search.
    // Implementing prefix search by changing compareTo() in CheckTuple does not
    // work.
    public static TreeSet<CheckTuple> getPrefixExpectedSubset(TreeSet<CheckTuple> checkTuples, CheckTuple lowKey,
            CheckTuple highKey) {
        TreeSet<CheckTuple> expectedSubset = new TreeSet<CheckTuple>();
        Iterator<CheckTuple> iter = checkTuples.iterator();
        while (iter.hasNext()) {
            CheckTuple t = iter.next();
            boolean geLowKey = true;
            boolean leHighKey = true;
            for (int i = 0; i < lowKey.getNumKeys(); i++) {
                if (t.get(i).compareTo(lowKey.get(i)) < 0) {
                    geLowKey = false;
                    break;
                }
            }
            for (int i = 0; i < highKey.getNumKeys(); i++) {
                if (t.get(i).compareTo(highKey.get(i)) > 0) {
                    leHighKey = false;
                    break;
                }
            }
            if (geLowKey && leHighKey) {
                expectedSubset.add(t);
            }
        }
        return expectedSubset;
    }

    @SuppressWarnings("unchecked")
    public void checkRangeSearch(ITreeIndexTestContext ctx, ITupleReference lowKey, ITupleReference highKey,
            boolean lowKeyInclusive, boolean highKeyInclusive) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Range Search.");
        }
        MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), lowKey);
        MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), highKey);
        IIndexCursor searchCursor = ctx.getIndexAccessor().createSearchCursor();
        RangePredicate rangePred = new RangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeyCmp,
                highKeyCmp);
        ctx.getIndexAccessor().search(searchCursor, rangePred);
        // Get the subset of elements from the expected set within given key
        // range.
        CheckTuple lowKeyCheck = createCheckTupleFromTuple(lowKey, ctx.getFieldSerdes(), lowKeyCmp.getKeyFieldCount());
        CheckTuple highKeyCheck = createCheckTupleFromTuple(highKey, ctx.getFieldSerdes(),
                highKeyCmp.getKeyFieldCount());
        NavigableSet<CheckTuple> expectedSubset = null;
        if (lowKeyCmp.getKeyFieldCount() < ctx.getKeyFieldCount()
                || highKeyCmp.getKeyFieldCount() < ctx.getKeyFieldCount()) {
            // Searching on a key prefix (low key or high key or both).
            expectedSubset = getPrefixExpectedSubset((TreeSet<CheckTuple>) ctx.getCheckTuples(), lowKeyCheck,
                    highKeyCheck);
        } else {
            // Searching on all key fields.
            expectedSubset = ((TreeSet<CheckTuple>) ctx.getCheckTuples()).subSet(lowKeyCheck, lowKeyInclusive,
                    highKeyCheck, highKeyInclusive);
        }
        Iterator<CheckTuple> checkIter = expectedSubset.iterator();
        int actualCount = 0;
        try {
            while (searchCursor.hasNext()) {
                if (!checkIter.hasNext()) {
                    fail("Range search returned more answers than expected.\nExpected: " + expectedSubset.size());
                }
                searchCursor.next();
                CheckTuple expectedTuple = checkIter.next();
                ITupleReference tuple = searchCursor.getTuple();
                compareActualAndExpected(tuple, expectedTuple, ctx.getFieldSerdes());
                actualCount++;
            }
            if (actualCount < expectedSubset.size()) {
                fail("Range search returned fewer answers than expected.\nExpected: " + expectedSubset.size()
                        + "\nActual  : " + actualCount);
            }
        } finally {
            searchCursor.close();
        }
    }

    public void checkPointSearches(ITreeIndexTestContext ictx) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Point Searches On All Expected Keys.");
        }
        OrderedIndexTestContext ctx = (OrderedIndexTestContext) ictx;
        IIndexCursor searchCursor = ctx.getIndexAccessor().createSearchCursor();

        ArrayTupleBuilder lowKeyBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        ArrayTupleReference lowKey = new ArrayTupleReference();
        ArrayTupleBuilder highKeyBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        ArrayTupleReference highKey = new ArrayTupleReference();
        RangePredicate rangePred = new RangePredicate(lowKey, highKey, true, true, null, null);

        // Iterate through expected tuples, and perform a point search in the
        // BTree to verify the tuple can be reached.
        for (CheckTuple checkTuple : ctx.getCheckTuples()) {
            createTupleFromCheckTuple(checkTuple, lowKeyBuilder, lowKey, ctx.getFieldSerdes());
            createTupleFromCheckTuple(checkTuple, highKeyBuilder, highKey, ctx.getFieldSerdes());
            MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), lowKey);
            MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), highKey);

            rangePred.setLowKey(lowKey, true);
            rangePred.setHighKey(highKey, true);
            rangePred.setLowKeyComparator(lowKeyCmp);
            rangePred.setHighKeyComparator(highKeyCmp);

            ctx.getIndexAccessor().search(searchCursor, rangePred);

            try {
                // We expect exactly one answer.
                if (searchCursor.hasNext()) {
                    searchCursor.next();
                    ITupleReference tuple = searchCursor.getTuple();
                    compareActualAndExpected(tuple, checkTuple, ctx.getFieldSerdes());
                }
                if (searchCursor.hasNext()) {
                    fail("Point search returned more than one answer.");
                }
            } finally {
                searchCursor.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void insertStringTuples(ITreeIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        for (int i = 0; i < numTuples; i++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), (Object[]) fieldValues);
            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());
                // Set expected values. Do this only after insertion succeeds
                // because we ignore duplicate keys.
                ctx.insertCheckTuple(createStringCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
            } catch (BTreeDuplicateKeyException e) {
                // Ignore duplicate key insertions.
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void bulkLoadStringTuples(ITreeIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        TreeSet<CheckTuple> tmpCheckTuples = new TreeSet<CheckTuple>();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            // Set expected values. We also use these as the pre-sorted stream
            // for bulk loading.
            ctx.insertCheckTuple(createStringCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    @SuppressWarnings("unchecked")
    public void updateTuples(ITreeIndexTestContext ictx, int numTuples, Random rnd) throws Exception {
        OrderedIndexTestContext ctx = (OrderedIndexTestContext) ictx;
        int fieldCount = ctx.getFieldCount();
        int keyFieldCount = ctx.getKeyFieldCount();
        // This is a noop because we can only update non-key fields.
        if (fieldCount == keyFieldCount) {
            return;
        }
        ArrayTupleBuilder updateTupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference updateTuple = new ArrayTupleReference();
        int numCheckTuples = ctx.getCheckTuples().size();
        // Copy CheckTuple references into array, so we can randomly pick from
        // there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        for (CheckTuple checkTuple : ctx.getCheckTuples()) {
            checkTuples[idx++] = checkTuple;
        }
        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Updating Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];
            // Update check tuple's non-key fields.
            for (int j = keyFieldCount; j < fieldCount; j++) {
                Comparable newValue = getRandomUpdateValue(ctx.getFieldSerdes()[j], rnd);
                checkTuple.set(j, newValue);
            }

            createTupleFromCheckTuple(checkTuple, updateTupleBuilder, updateTuple, ctx.getFieldSerdes());
            ctx.getIndexAccessor().update(updateTuple);

            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;
        }
    }

    public CheckTuple createStringCheckTuple(String[] fieldValues, int numKeyFields) {
        CheckTuple<String> checkTuple = new CheckTuple<String>(fieldValues.length, numKeyFields);
        for (String s : fieldValues) {
            checkTuple.add((String) s);
        }
        return checkTuple;
    }

    private static Comparable getRandomUpdateValue(ISerializerDeserializer serde, Random rnd) {
        if (serde instanceof IntegerSerializerDeserializer) {
            return Integer.valueOf(rnd.nextInt());
        } else if (serde instanceof UTF8StringSerializerDeserializer) {
            return getRandomString(10, rnd);
        }
        return null;
    }

    public static String getRandomString(int length, Random rnd) {
        String s = Long.toHexString(Double.doubleToLongBits(rnd.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < length; i++) {
            strBuilder.append(s.charAt(Math.abs(rnd.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }

    @Override
    protected CheckTuple createCheckTuple(int numFields, int numKeyFields) {
        return new CheckTuple(numFields, numKeyFields);
    }

    @Override
    protected ISearchPredicate createNullSearchPredicate() {
        return new RangePredicate(null, null, true, true, null, null);
    }

    @Override
    public void checkExpectedResults(ITreeIndexCursor cursor, Collection checkTuples,
            ISerializerDeserializer[] fieldSerdes, int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception {
        int actualCount = 0;
        try {
            while (cursor.hasNext()) {
                if (!checkIter.hasNext()) {
                    fail("Ordered scan returned more answers than expected.\nExpected: " + checkTuples.size());
                }
                cursor.next();
                CheckTuple expectedTuple = checkIter.next();
                ITupleReference tuple = cursor.getTuple();
                compareActualAndExpected(tuple, expectedTuple, fieldSerdes);
                actualCount++;
            }
            if (actualCount < checkTuples.size()) {
                fail("Ordered scan returned fewer answers than expected.\nExpected: " + checkTuples.size()
                        + "\nActual  : " + actualCount);
            }
        } finally {
            cursor.close();
        }

    }

    @Override
    protected CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields) {
        CheckTuple<Integer> checkTuple = new CheckTuple<Integer>(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.add(v);
        }
        return checkTuple;
    }

    @Override
    protected void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd) {
        for (int j = 0; j < numKeyFields; j++) {
            fieldValues[j] = rnd.nextInt() % maxValue;
        }
    }

    @Override
    protected void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = j;
        }
    }

    @Override
    protected Collection createCheckTuplesCollection() {
        return new TreeSet<CheckTuple>();
    }

    @Override
    protected ArrayTupleBuilder createDeleteTupleBuilder(ITreeIndexTestContext ctx) {
        return new ArrayTupleBuilder(ctx.getKeyFieldCount());
    }

    @Override
    protected boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple, ITreeIndexTestContext ctx)
            throws HyracksDataException {
        @SuppressWarnings("unchecked")
        TreeSet<CheckTuple> checkTuples = (TreeSet<CheckTuple>) ctx.getCheckTuples();
        CheckTuple matchingCheckTuple = checkTuples.floor(checkTuple);
        if (matchingCheckTuple == null) {
            return false;
        }
        compareActualAndExpected(tuple, matchingCheckTuple, ctx.getFieldSerdes());
        return true;
    }
}