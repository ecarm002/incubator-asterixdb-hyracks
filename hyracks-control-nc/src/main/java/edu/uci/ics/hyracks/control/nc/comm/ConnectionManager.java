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
package edu.uci.ics.hyracks.control.nc.comm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.FrameConstants;
import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListener;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListenerProvider;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.comm.PartitionId;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.resources.IResourceDeallocator;

public class ConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());

    private static final int INITIAL_MESSAGE_LEN = 20;

    private NetworkAddress networkAddress;

    private ServerSocketChannel serverSocketChannel;

    private final IHyracksContext ctx;

    private final Map<UUID, IDataReceiveListenerProvider> pendingConnectionReceivers;

    private final Map<UUID, Map<PartitionId, Run>> runMap;

    private final ConnectionListenerThread connectionListenerThread;

    private final DataListenerThread dataListenerThread;

    private final IDataReceiveListener initialDataReceiveListener;

    private final Set<IConnectionEntry> connections;

    private volatile boolean stopped;

    private ByteBuffer emptyFrame;

    public ConnectionManager(IHyracksContext ctx, InetAddress address) throws IOException {
        this.ctx = ctx;
        serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(address, 0));

        networkAddress = new NetworkAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Connection manager listening on " + serverSocket.getInetAddress() + ":"
                    + serverSocket.getLocalPort());
        }

        pendingConnectionReceivers = new HashMap<UUID, IDataReceiveListenerProvider>();
        runMap = new HashMap<UUID, Map<PartitionId, Run>>();
        dataListenerThread = new DataListenerThread();
        connectionListenerThread = new ConnectionListenerThread();
        initialDataReceiveListener = new InitialDataReceiveListener();
        emptyFrame = ctx.getResourceManager().allocateFrame();
        emptyFrame.putInt(FrameHelper.getTupleCountOffset(ctx), 0);
        connections = new HashSet<IConnectionEntry>();
    }

    public synchronized void dumpStats() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Number of pendingConnectionReceivers: " + pendingConnectionReceivers.size());
            LOGGER.info("Number of selectable keys: " + dataListenerThread.selector.keys().size());
        }
    }

    public NetworkAddress getNetworkAddress() {
        return networkAddress;
    }

    public void start() {
        stopped = false;
        connectionListenerThread.start();
        dataListenerThread.start();
    }

    public void stop() {
        try {
            stopped = true;
            serverSocketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IFrameWriter createPartitionWriter(IResourceDeallocator deallocator, UUID jobId, PartitionId partitionId)
            throws HyracksDataException {
        final Run run;
        try {
            run = new Run(ctx.getResourceManager().createFile(
                    ConnectionManager.class.getName() + "_" + jobId + "_" + partitionId, ".run"));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        deallocator.addDeallocatableResource(run);
        registerRun(jobId, partitionId, run);

        return new IFrameWriter() {
            @Override
            public void open() throws HyracksDataException {
                // do nothing
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                run.write(buffer);
            }

            @Override
            public void flush() throws HyracksDataException {
                // do nothing
            }

            @Override
            public void close() throws HyracksDataException {
                run.close();
            }
        };
    }

    private synchronized void registerRun(UUID jobId, PartitionId partitionId, Run run) {
        Map<PartitionId, Run> partitionMap = runMap.get(jobId);
        if (partitionMap == null) {
            partitionMap = new HashMap<PartitionId, Run>();
            runMap.put(jobId, partitionMap);
        }
        partitionMap.put(partitionId, run);
        notifyAll();
    }

    public IFrameWriter connect(NetworkAddress address, UUID id, int senderId) throws HyracksDataException {
        try {
            SocketChannel channel = SocketChannel
                    .open(new InetSocketAddress(address.getIpAddress(), address.getPort()));
            byte[] initialFrame = new byte[INITIAL_MESSAGE_LEN];
            ByteBuffer buffer = ByteBuffer.wrap(initialFrame);
            buffer.clear();
            buffer.putLong(id.getMostSignificantBits());
            buffer.putLong(id.getLeastSignificantBits());
            buffer.putInt(senderId);
            buffer.flip();
            int bytesWritten = 0;
            while (bytesWritten < INITIAL_MESSAGE_LEN) {
                int n = channel.write(buffer);
                if (n < 0) {
                    throw new HyracksDataException("Stream closed prematurely");
                }
                bytesWritten += n;
            }
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Send Initial message: " + id + ":" + senderId);
            }
            buffer.clear();
            buffer.limit(FrameConstants.SIZE_LEN);
            int bytesRead = 0;
            while (bytesRead < FrameConstants.SIZE_LEN) {
                int n = channel.read(buffer);
                if (n < 0) {
                    throw new HyracksDataException("Stream closed prematurely");
                }
                bytesRead += n;
            }
            buffer.flip();
            int frameLen = buffer.getInt();
            if (frameLen != FrameConstants.SIZE_LEN) {
                throw new IllegalStateException("Received illegal framelen = " + frameLen);
            }
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Got Ack message: " + id + ":" + senderId);
            }
            return new NetworkFrameWriter(channel);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public synchronized void acceptConnection(UUID id, IDataReceiveListenerProvider receiver) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.info("Connection manager accepting " + id);
        }
        pendingConnectionReceivers.put(id, receiver);
    }

    public synchronized void unacceptConnection(UUID id) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.info("Connection manager unaccepting " + id);
        }
        pendingConnectionReceivers.remove(id);
    }

    public synchronized void abortConnections(UUID jobId, UUID stageId) {
        List<IConnectionEntry> abortConnections = new ArrayList<IConnectionEntry>();
        synchronized (this) {
            for (IConnectionEntry ce : connections) {
                if (((ConnectionEntry) ce).getJobId().equals(jobId)
                        && ((ConnectionEntry) ce).getStageId().equals(stageId)) {
                    abortConnections.add(ce);
                }
            }
        }
        dataListenerThread.addPendingAbortConnections(abortConnections);
    }

    private final class NetworkFrameWriter implements IFrameWriter {
        private SocketChannel channel;

        NetworkFrameWriter(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                synchronized (emptyFrame) {
                    emptyFrame.position(0);
                    emptyFrame.limit(emptyFrame.capacity());
                    channel.write(emptyFrame);
                }
                channel.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                if (LOGGER.isLoggable(Level.FINER)) {
                    int frameLen = buffer.getInt(buffer.position());
                    LOGGER.finer("ConnectionManager.NetworkFrameWriter: frameLen = " + frameLen);
                }
                while (buffer.remaining() > 0) {
                    channel.write(buffer);
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void open() throws HyracksDataException {
        }

        @Override
        public void flush() {
        }
    }

    private final class ConnectionListenerThread extends Thread {
        public ConnectionListenerThread() {
            super("Hyracks Connection Listener Thread");
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    SocketChannel sc = serverSocketChannel.accept();
                    dataListenerThread.addSocketChannel(sc);
                } catch (AsynchronousCloseException e) {
                    // do nothing
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private final class DataListenerThread extends Thread {
        private Selector selector;

        private List<SocketChannel> pendingNewSockets;
        private List<IConnectionEntry> pendingAbortConnections;

        public DataListenerThread() {
            super("Hyracks Data Listener Thread");
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            pendingNewSockets = new ArrayList<SocketChannel>();
            pendingAbortConnections = new ArrayList<IConnectionEntry>();
        }

        synchronized void addSocketChannel(SocketChannel sc) throws IOException {
            pendingNewSockets.add(sc);
            selector.wakeup();
        }

        synchronized void addPendingAbortConnections(List<IConnectionEntry> abortConnections) {
            pendingAbortConnections.addAll(abortConnections);
            selector.wakeup();
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("Starting Select");
                    }
                    int n = selector.select();
                    synchronized (this) {
                        if (!pendingNewSockets.isEmpty()) {
                            for (SocketChannel sc : pendingNewSockets) {
                                sc.configureBlocking(false);
                                SelectionKey scKey = sc.register(selector, SelectionKey.OP_READ);
                                ConnectionEntry entry = new ConnectionEntry(ctx, sc, scKey);
                                entry.setDataReceiveListener(initialDataReceiveListener);
                                scKey.attach(entry);
                                if (LOGGER.isLoggable(Level.FINE)) {
                                    LOGGER.fine("Woke up selector");
                                }
                            }
                            pendingNewSockets.clear();
                        }
                        if (!pendingAbortConnections.isEmpty()) {
                            for (IConnectionEntry cei : pendingAbortConnections) {
                                ConnectionEntry ce = (ConnectionEntry) cei;
                                SelectionKey key = ce.getSelectionKey();
                                ce.abort();
                                ((ConnectionEntry) ce).dispatch(key);
                                key.cancel();
                                ce.close();
                                synchronized (ConnectionManager.this) {
                                    connections.remove(ce);
                                }
                            }
                            pendingAbortConnections.clear();
                        }
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("Selector: " + n);
                        }
                        if (n > 0) {
                            for (Iterator<SelectionKey> i = selector.selectedKeys().iterator(); i.hasNext();) {
                                SelectionKey key = i.next();
                                i.remove();
                                ConnectionEntry entry = (ConnectionEntry) key.attachment();
                                boolean close = false;
                                try {
                                    close = entry.dispatch(key);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    close = true;
                                }
                                if (close) {
                                    key.cancel();
                                    entry.close();
                                    synchronized (ConnectionManager.this) {
                                        connections.remove(entry);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class InitialDataReceiveListener implements IDataReceiveListener {
        @Override
        public void dataReceived(IConnectionEntry entry) throws IOException {
            ByteBuffer buffer = entry.getReadBuffer();
            buffer.flip();
            IDataReceiveListener newListener = null;
            ConnectionEntry ce = (ConnectionEntry) entry;
            if (buffer.remaining() >= INITIAL_MESSAGE_LEN) {
                long msb = buffer.getLong();
                long lsb = buffer.getLong();
                UUID endpointID = new UUID(msb, lsb);
                int senderId = buffer.getInt();
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Initial Frame received: " + endpointID + ":" + senderId);
                }
                IDataReceiveListenerProvider connectionReceiver;
                synchronized (ConnectionManager.this) {
                    connectionReceiver = pendingConnectionReceivers.get(endpointID);
                    if (connectionReceiver == null) {
                        ce.close();
                        return;
                    }
                }

                newListener = connectionReceiver.getDataReceiveListener();
                ce.setDataReceiveListener(newListener);
                ce.setJobId(connectionReceiver.getJobId());
                ce.setStageId(connectionReceiver.getStageId());
                synchronized (ConnectionManager.this) {
                    connections.add(entry);
                }
                byte[] ack = new byte[4];
                ByteBuffer ackBuffer = ByteBuffer.wrap(ack);
                ackBuffer.clear();
                ackBuffer.putInt(FrameConstants.SIZE_LEN);
                ackBuffer.flip();
                ce.write(ackBuffer);
            }
            buffer.compact();
            if (newListener != null && buffer.remaining() > 0) {
                newListener.dataReceived(entry);
            }
        }

        @Override
        public void eos(IConnectionEntry entry) {
        }
    }
}