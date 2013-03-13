package edu.uci.ics.genomix.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

/**
 * count
 * 
 */
public class MergeKmerAggregateFactory implements IAggregatorDescriptorFactory {
	private static final long serialVersionUID = 1L;
	private static final int MAX = 127;

	public MergeKmerAggregateFactory() {
	}

	@Override
	public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
			RecordDescriptor inRecordDescriptor,
			RecordDescriptor outRecordDescriptor, int[] keyFields,
			int[] keyFieldsInPartialResults) throws HyracksDataException {
		return new IAggregatorDescriptor() {

			@Override
			public void reset() {
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub

			}

			@Override
			public AggregateState createAggregateStates() {
				// TODO Auto-generated method stub
				return new AggregateState(new Object() {
				});
			}

			private byte getField(IFrameTupleAccessor accessor, int tIndex,
					int fieldId) {
				int tupleOffset = accessor.getTupleStartOffset(tIndex);
				int fieldStart = accessor.getFieldStartOffset(tIndex, fieldId);
				int offset = tupleOffset + fieldStart
						+ accessor.getFieldSlotsLength();
				byte data = ByteSerializerDeserializer.getByte(accessor
						.getBuffer().array(), offset);
				return data;
			}

			@Override
			public void init(ArrayTupleBuilder tupleBuilder,
					IFrameTupleAccessor accessor, int tIndex,
					AggregateState state) throws HyracksDataException {
				byte bitmap = getField(accessor, tIndex, 1);
				byte count = 1;

				DataOutput fieldOutput = tupleBuilder.getDataOutput();
				try {
					fieldOutput.writeByte(bitmap);
					tupleBuilder.addFieldEndOffset();
					fieldOutput.writeByte(count);
					tupleBuilder.addFieldEndOffset();
				} catch (IOException e) {
					throw new HyracksDataException(
							"I/O exception when initializing the aggregator.");
				}

			}

			@Override
			public void aggregate(IFrameTupleAccessor accessor, int tIndex,
					IFrameTupleAccessor stateAccessor, int stateTupleIndex,
					AggregateState state) throws HyracksDataException {
				byte bitmap = getField(accessor, tIndex, 1);
				short count = 1;

				int statetupleOffset = stateAccessor
						.getTupleStartOffset(stateTupleIndex);
				int statefieldStart = stateAccessor.getFieldStartOffset(
						stateTupleIndex, 1);
				int stateoffset = statetupleOffset
						+ stateAccessor.getFieldSlotsLength() + statefieldStart;

				byte[] data = stateAccessor.getBuffer().array();

				bitmap |= data[stateoffset];
				count += data[stateoffset + 1];
				if (count >= MAX) {
					count = (byte) MAX;
				}
				data[stateoffset] = bitmap;
				data[stateoffset + 1] = (byte) count;
			}

			@Override
			public void outputPartialResult(ArrayTupleBuilder tupleBuilder,
					IFrameTupleAccessor accessor, int tIndex,
					AggregateState state) throws HyracksDataException {
				byte bitmap = getField(accessor, tIndex, 1);
				byte count = getField(accessor, tIndex, 2);
				DataOutput fieldOutput = tupleBuilder.getDataOutput();
				try {
					fieldOutput.writeByte(bitmap);
					tupleBuilder.addFieldEndOffset();
					fieldOutput.writeByte(count);
					tupleBuilder.addFieldEndOffset();
				} catch (IOException e) {
					throw new HyracksDataException(
							"I/O exception when writing aggregation to the output buffer.");
				}

			}

			@Override
			public void outputFinalResult(ArrayTupleBuilder tupleBuilder,
					IFrameTupleAccessor accessor, int tIndex,
					AggregateState state) throws HyracksDataException {
				outputPartialResult(tupleBuilder, accessor, tIndex, state);
			}

		};
	}

}