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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.join.GraceHashGroupJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.HybridHashGroupJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashGroupJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;

public class TPCHCustomerOrderGroupJoinTest extends AbstractIntegrationTest {
    private static final long serialVersionUID = 1L;
    private static final boolean DEBUG = true;
    private static class NoopNullWriterFactory implements INullWriterFactory {

        public static final NoopNullWriterFactory INSTANCE = new NoopNullWriterFactory();

        private NoopNullWriterFactory() {
        }

        @Override
        public INullWriter createNullWriter() {
            return new INullWriter() {
                @Override
                public void writeNull(DataOutput out) throws HyracksDataException {
                    try {
                        out.writeInt(-1);
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            };
        }
    }

    /*
     * TPCH Customer table: CREATE TABLE CUSTOMER ( C_CUSTKEY INTEGER NOT NULL,
     * C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY
     * INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT
     * NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL );
     * 
     * TPCH Orders table: CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
     * O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE
     * DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY
     * CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT
     * NULL, O_COMMENT VARCHAR(79) NOT NULL );
     */

    @Test
    public void customerOrderInMemoryGroupJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "/media/New Volume_/tpch/s0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "/media/New Volume_/tpch/s0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderGroupJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);
        
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[1];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }
        
        InMemoryHashGroupJoinOperatorDescriptor groupJoin = new InMemoryHashGroupJoinOperatorDescriptor(
                spec,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                custOrderGroupJoinDesc, nullWriterFactories, 128);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, groupJoin, NC1_ID);

        HashGroupOperatorDescriptor finalGroup = new HashGroupOperatorDescriptor(
        		spec, new int[] { 1 },
        		new FieldHashPartitionComputerFactory( new int[] { 1 }, new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }),
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                custOrderGroupJoinDesc, 128	
        		);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);

/*        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
*/        
        
        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, groupJoin, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, groupJoin, 1);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, groupJoin, 0, printer, 0);

        IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(finalGroupConn, groupJoin, 0, finalGroup, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, finalGroup, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderGraceHashGroupJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "/media/New Volume_/tpch/s0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "/media/New Volume_/tpch/s0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderGroupJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);
        
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[1];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }        
        
        GraceHashGroupJoinOperatorDescriptor groupJoin = new GraceHashGroupJoinOperatorDescriptor(
                spec,
                4,
                10,
                200,
                1.2,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                custOrderGroupJoinDesc, nullWriterFactories);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, groupJoin, NC1_ID);

        HashGroupOperatorDescriptor finalGroup = new HashGroupOperatorDescriptor(
        		spec, new int[] { 1 },
        		new FieldHashPartitionComputerFactory( new int[] { 1 }, new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }),
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                custOrderGroupJoinDesc, 128	
        		);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);
        
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        
        
//        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, groupJoin, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, groupJoin, 1);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, groupJoin, 0, printer, 0);

        IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(finalGroupConn, groupJoin, 0, finalGroup, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, finalGroup, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderHybridHashGroupJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "/media/New Volume_/tpch/s0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "/media/New Volume_/tpch/s0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderGroupJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);
        
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[1];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }        
        
        HybridHashGroupJoinOperatorDescriptor groupJoin = new HybridHashGroupJoinOperatorDescriptor(
                spec,
                4,
                10,
                200,
                1.2,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                custOrderGroupJoinDesc, nullWriterFactories);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, groupJoin, NC1_ID);

/*        HashGroupOperatorDescriptor finalGroup = new HashGroupOperatorDescriptor(
        		spec, new int[] { 1 },
        		new FieldHashPartitionComputerFactory( new int[] { 1 }, new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }),
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                custOrderGroupJoinDesc, 128	
        		);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);
*/        
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        
        
//        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, groupJoin, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, groupJoin, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, groupJoin, 0, printer, 0);

/*        IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(finalGroupConn, groupJoin, 0, finalGroup, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, finalGroup, 0, printer, 0);
*/
        spec.addRoot(printer);
        runTest(spec);
    }

/*    @Test
    public void customerOrderCIDGraceJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        GraceHashJoinOperatorDescriptor join = new GraceHashJoinOperatorDescriptor(
                spec,
                4,
                10,
                200,
                1.2,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        HybridHashJoinOperatorDescriptor join = new HybridHashJoinOperatorDescriptor(
                spec,
                5,
                20,
                200,
                1.2,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDInMemoryHashLeftOuterJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[ordersDesc.getFields().length];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(
                spec,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, true, nullWriterFactories, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDGraceHashLeftOuterJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[ordersDesc.getFields().length];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }

        GraceHashJoinOperatorDescriptor join = new GraceHashJoinOperatorDescriptor(
                spec,
                5,
                20,
                200,
                1.2,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, true, nullWriterFactories);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashLeftOuterJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[ordersDesc.getFields().length];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }

        HybridHashJoinOperatorDescriptor join = new HybridHashJoinOperatorDescriptor(
                spec,
                5,
                20,
                200,
                1.2,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, true, nullWriterFactories);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinMulti() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(
                spec,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDGraceJoinMulti() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        GraceHashJoinOperatorDescriptor join = new GraceHashJoinOperatorDescriptor(
                spec,
                3,
                20,
                100,
                1.2,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoinMulti() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        HybridHashJoinOperatorDescriptor join = new HybridHashJoinOperatorDescriptor(
                spec,
                3,
                20,
                100,
                1.2,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinAutoExpand() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(
                spec,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, 128);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, join, 2);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinMultiMaterialized() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        MaterializingOperatorDescriptor ordMat = new MaterializingOperatorDescriptor(spec, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordMat, NC1_ID, NC2_ID);

        MaterializingOperatorDescriptor custMat = new MaterializingOperatorDescriptor(spec, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custMat, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(
                spec,
                new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordPartConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordPartConn, ordScanner, 0, ordMat, 0);

        IConnectorDescriptor custPartConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(custPartConn, custScanner, 0, custMat, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordMat, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custMat, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
*/
}