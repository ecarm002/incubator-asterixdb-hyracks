package edu.uci.ics.hyracks.storage.am.common.impls;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public abstract class AbstractTreeIndex implements ITreeIndex {
	
	protected final static int rootPage = 1;
	
    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final IFreePageManager freePageManager;
    protected final IBufferCache bufferCache;
    protected final int fieldCount;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final ReadWriteLock treeLatch;
    protected int fileId;
	
    public AbstractTreeIndex(IBufferCache bufferCache, int fieldCount, IBinaryComparatorFactory[] cmpFactories, IFreePageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fieldCount = fieldCount;
        this.cmpFactories = cmpFactories;       
        this.freePageManager = freePageManager;
        this.treeLatch = new ReentrantReadWriteLock(true);
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
    }
    
    public boolean isEmptyTree(ITreeIndexFrame leafFrame) throws HyracksDataException {
    	ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            leafFrame.setPage(rootNode);
            if (leafFrame.getLevel() == 0 && leafFrame.getTupleCount() == 0) {
            	return true;
            } else {
            	return false;
            }
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }
	
	public abstract class AbstractTreeIndexBulkLoader implements ITreeIndexBulkLoader {
		protected final MultiComparator cmp;
	    protected final int slotSize;
	    protected final int leafMaxBytes;
	    protected final int interiorMaxBytes;
	    protected final ISplitKey splitKey;
	    // we maintain a frontier of nodes for each level
	    protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
	    protected final ITreeIndexMetaDataFrame metaFrame;
	    protected final ITreeIndexTupleWriter tupleWriter;
	    
		protected ITreeIndexFrame leafFrame, interiorFrame;
	    
	    public AbstractTreeIndexBulkLoader(float fillFactor) throws TreeIndexException, HyracksDataException {
            leafFrame = leafFrameFactory.createFrame();
            interiorFrame = interiorFrameFactory.createFrame();
            metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
            
            if (!isEmptyTree(leafFrame)) {
	    		throw new TreeIndexException("Trying to Bulk-load a non-empty BTree.");
	    	}
            
            this.cmp = MultiComparator.create(cmpFactories);
            
	    	leafFrame.setMultiComparator(cmp);
        	interiorFrame.setMultiComparator(cmp);
        	
            splitKey = createSplitKey(leafFrame.getTupleWriter().createTupleReference());
            splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId),
                    true);
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);

            slotSize = leafFrame.getSlotSize();

            nodeFrontiers.add(leafFrontier);
	    }

		@Override
	    public void add(ITupleReference tuple) throws HyracksDataException {
	        NodeFrontier leafFrontier = nodeFrontiers.get(0);

	        int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
	        int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

	        // try to free space by compression
	        if (spaceUsed + spaceNeeded > leafMaxBytes) {
	            leafFrame.compress();
	            spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
	        }

	        if (spaceUsed + spaceNeeded > leafMaxBytes) {
	            leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
	            int splitKeySize = tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount());
	            splitKey.initData(splitKeySize);
	            tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount(),
	                    splitKey.getBuffer().array(), 0);
	            splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);
	            splitKey.setLeftPage(leafFrontier.pageId);
	            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);

	            leafFrame.setNextLeaf(leafFrontier.pageId);
	            leafFrontier.page.releaseWriteLatch();
	            bufferCache.unpin(leafFrontier.page);

	            splitKey.setRightPage(leafFrontier.pageId);
	            propagateBulk(1);

	            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId),
	                    true);
	            leafFrontier.page.acquireWriteLatch();
	            leafFrame.setPage(leafFrontier.page);
	            leafFrame.initBuffer((byte) 0);
	        }

	        leafFrame.setPage(leafFrontier.page);
	        leafFrame.insertSorted(tuple);
	    }

	    @Override
	    public void end() throws HyracksDataException {
	    	// copy root
	        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
	        rootNode.acquireWriteLatch();
	        NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
	        try {
	            ICachedPage toBeRoot = lastNodeFrontier.page;
	            System.arraycopy(toBeRoot.getBuffer().array(), 0, rootNode.getBuffer().array(), 0, toBeRoot.getBuffer()
	                    .capacity());
	        } finally {
	            rootNode.releaseWriteLatch();
	            bufferCache.unpin(rootNode);

	            // register old root as free page
	            freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);

	            // make old root a free page
	            interiorFrame.setPage(lastNodeFrontier.page);
	            interiorFrame.initBuffer(freePageManager.getFreePageLevelIndicator());

	            // cleanup
	            for (int i = 0; i < nodeFrontiers.size(); i++) {
	                nodeFrontiers.get(i).page.releaseWriteLatch();
	                bufferCache.unpin(nodeFrontiers.get(i).page);
	            }
	        }
	    }
	    
	    private void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.pageId = freePageManager.getFreePage(metaFrame);
            frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }
	    
	    private void propagateBulk(int level) throws HyracksDataException {

	        if (splitKey.getBuffer() == null)
	            return;

	        if (level >= nodeFrontiers.size())
	            addLevel();

	        NodeFrontier frontier = nodeFrontiers.get(level);
	        interiorFrame.setPage(frontier.page);

	        ITupleReference tuple = splitKey.getTuple();
	        int spaceNeeded = tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount()) + slotSize + 4;
	        int spaceUsed = interiorFrame.getBuffer().capacity() - interiorFrame.getTotalFreeSpace();
	        if (spaceUsed + spaceNeeded > interiorMaxBytes) {

	            ISplitKey copyKey = splitKey.duplicate(leafFrame.getTupleWriter().createTupleReference());
	            tuple = copyKey.getTuple();

	            frontier.lastTuple.resetByTupleIndex(interiorFrame, interiorFrame.getTupleCount() - 1);
	            int splitKeySize = tupleWriter.bytesRequired(frontier.lastTuple, 0, cmp.getKeyFieldCount());
	            splitKey.initData(splitKeySize);
	            tupleWriter
	                    .writeTupleFields(frontier.lastTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
	            splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);
	            splitKey.setLeftPage(frontier.pageId);

	            interiorFrame.deleteGreatest();

	            frontier.page.releaseWriteLatch();
	            bufferCache.unpin(frontier.page);
	            frontier.pageId = freePageManager.getFreePage(metaFrame);

	            splitKey.setRightPage(frontier.pageId);
	            propagateBulk(level + 1);

	            frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
	            frontier.page.acquireWriteLatch();
	            interiorFrame.setPage(frontier.page);
	            interiorFrame.initBuffer((byte) level);
	        }
	        interiorFrame.insertSorted(tuple);
	    }
	    
	    protected abstract ISplitKey createSplitKey(ITreeIndexTupleReference tuple);
	}
}
