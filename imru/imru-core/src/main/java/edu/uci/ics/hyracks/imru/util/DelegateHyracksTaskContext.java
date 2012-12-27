package edu.uci.ics.hyracks.imru.util;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;

/**
 * Base class for classes that specialize an existing
 * IHyracksTaskContext implementation, because many
 * implementations are not subclassable.
 */
public class DelegateHyracksTaskContext implements IHyracksTaskContext {

    private final IHyracksTaskContext delegate;

    /**
     * Construct a new DelegateHyracksTaskContext.
     *
     * @param delegate
     *            The task context to delegate calls to.
     */
    public DelegateHyracksTaskContext(IHyracksTaskContext delegate) {
        this.delegate = delegate;
    }

    @Override
    public ByteBuffer allocateFrame() {
        return delegate.allocateFrame();
    }

    @Override
    public int getFrameSize() {
        return delegate.getFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return delegate.getIOManager();
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return delegate.createUnmanagedWorkspaceFile(prefix);
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        return delegate.createManagedWorkspaceFile(prefix);
    }

    @Override
    public void registerDeallocatable(IDeallocatable deallocatable) {
        delegate.registerDeallocatable(deallocatable);
    }

    @Override
    public void setStateObject(IStateObject taskState) {
        delegate.setStateObject(taskState);
    }

    @Override
    public IStateObject getStateObject(Object taskId) {
        return delegate.getStateObject(taskId);
    }

    @Override
    public IHyracksJobletContext getJobletContext() {
        return delegate.getJobletContext();
    }

    @Override
    public TaskAttemptId getTaskAttemptId() {
        return delegate.getTaskAttemptId();
    }

    @Override
    public ICounterContext getCounterContext() {
        return delegate.getCounterContext();
    }

     @Override
    public void sendApplicationMessageToCC(byte[] message, String nodeId)
            throws Exception {
        delegate.sendApplicationMessageToCC(message, nodeId);
    }
}