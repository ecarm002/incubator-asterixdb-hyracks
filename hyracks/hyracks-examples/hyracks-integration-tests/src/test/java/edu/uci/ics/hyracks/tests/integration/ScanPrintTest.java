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
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.hash.IntegerBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;

public class ScanPrintTest extends AbstractIntegrationTest {
    @Test
    public void scanPrint01() throws Exception {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC2_ID, new FileReference(new File("data/words.txt"))),
                new FileSplit(NC1_ID, new FileReference(new File("data/words.txt"))) });

        RecordDescriptor desc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
                spec,
                splitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','),
                desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID, NC1_ID);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, csvScanner, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void scanPrint02() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, ordScanner, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void scanPrint03() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { IntegerBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, ordScanner, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}