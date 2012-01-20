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
package edu.uci.ics.hyracks.tests.comm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataReader;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameDeserializingDataReader;
import edu.uci.ics.hyracks.dataflow.common.comm.io.SerializingDataWriter;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.test.support.TestJobletContext;
import edu.uci.ics.hyracks.test.support.TestNCApplicationContext;
import edu.uci.ics.hyracks.test.support.TestRootContext;
import edu.uci.ics.hyracks.test.support.TestStageletContext;

public class SerializationDeserializationTest {
    private static final String DBLP_FILE = "data/dblp.txt";

    private static class SerDeserRunner {
        private final IHyracksStageletContext ctx;
        private static final int FRAME_SIZE = 32768;
        private RecordDescriptor rDes;
        private List<ByteBuffer> buffers;

        public SerDeserRunner(RecordDescriptor rDes) throws HyracksException {
            IHyracksRootContext rootCtx = new TestRootContext(FRAME_SIZE);
            INCApplicationContext appCtx = new TestNCApplicationContext(rootCtx, null);
            IHyracksJobletContext jobletCtx = new TestJobletContext(appCtx, UUID.randomUUID(), 0);
            ctx = new TestStageletContext(jobletCtx, UUID.randomUUID());
            this.rDes = rDes;
            buffers = new ArrayList<ByteBuffer>();
        }

        public IOpenableDataWriter<Object[]> createWriter() {
            return new SerializingDataWriter(ctx, rDes, new IFrameWriter() {
                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer toBuf = ctx.allocateFrame();
                    toBuf.put(buffer);
                    buffers.add(toBuf);
                }

                @Override
                public void close() throws HyracksDataException {

                }

                @Override
                public void flush() throws HyracksDataException {
                }
            });
        }

        public IOpenableDataReader<Object[]> createDataReader() {
            return new FrameDeserializingDataReader(ctx, new IFrameReader() {
                private int i;

                @Override
                public void open() throws HyracksDataException {
                    i = 0;
                }

                @Override
                public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (i < buffers.size()) {
                        ByteBuffer buf = buffers.get(i);
                        buf.flip();
                        buffer.put(buf);
                        buffer.flip();
                        ++i;
                        return true;
                    }
                    return false;
                }

                @Override
                public void close() throws HyracksDataException {

                }
            }, rDes);
        }
    }

    private interface LineProcessor {
        public void process(String line, IDataWriter<Object[]> writer) throws Exception;
    }

    private void run(RecordDescriptor rDes, LineProcessor lp) throws Exception {
        SerDeserRunner runner = new SerDeserRunner(rDes);
        IOpenableDataWriter<Object[]> writer = runner.createWriter();
        writer.open();
        BufferedReader in = new BufferedReader(new FileReader(DBLP_FILE));
        String line;
        while ((line = in.readLine()) != null) {
            lp.process(line, writer);
        }
        writer.close();

        IOpenableDataReader<Object[]> reader = runner.createDataReader();
        reader.open();
        Object[] arr;
        while ((arr = reader.readData()) != null) {
            System.err.println(arr[0] + " " + arr[1]);
        }
        reader.close();
    }

    @Test
    public void serdeser01() throws Exception {
        RecordDescriptor rDes = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        LineProcessor processor = new LineProcessor() {
            @Override
            public void process(String line, IDataWriter<Object[]> writer) throws Exception {
                String[] splits = line.split(" ");
                for (String s : splits) {
                    writer.writeData(new Object[] { s, Integer.valueOf(1) });
                }
            }
        };
        run(rDes, processor);
    }
}