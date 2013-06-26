package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.AdjMessage;
import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class MessageWritable implements WritableComparable<MessageWritable> {
    /**
     * sourceVertexId stores source vertexId when headVertex sends the message
     * stores neighber vertexValue when pathVertex sends the message
     * file stores the point to the file that stores the chains of connected DNA
     */
    private PositionWritable sourceVertexId;
    private KmerBytesWritable chainVertexId;
    private AdjacencyListWritable neighberNode; //incoming or outgoing
    private byte message;
    private byte adjMessage;

    private byte checkMessage;

    public MessageWritable() {
        sourceVertexId = new PositionWritable();
        chainVertexId = new KmerBytesWritable(0);
        neighberNode = new AdjacencyListWritable();
        message = Message.NON;
        adjMessage = AdjMessage.NON;
        checkMessage = (byte) 0;
    }
    
    public void set(MessageWritable msg) {
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(msg.getSourceVertexId());
        }
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.chainVertexId.set(msg.getChainVertexId());
        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(msg.getNeighberNode());
        }
        checkMessage |= CheckMessage.ADJMSG;
        this.adjMessage = msg.getAdjMessage();
        this.message = msg.getMessage();
    }

    public void set(PositionWritable sourceVertexId, KmerBytesWritable chainVertexId, AdjacencyListWritable neighberNode, byte message) {
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(sourceVertexId.getReadID(),sourceVertexId.getPosInRead());
        }
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.chainVertexId.set(chainVertexId);
        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(neighberNode);
        }
        this.message = message;
    }

    public void reset() {
        checkMessage = 0;
        chainVertexId.reset(1);
        neighberNode.reset();
        message = Message.NON;
    }

    public PositionWritable getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(PositionWritable sourceVertexId) {
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(sourceVertexId.getReadID(),sourceVertexId.getPosInRead());
        }
    }
    
    public KmerBytesWritable getChainVertexId() {
        return chainVertexId;
    }

    public void setChainVertexId(KmerBytesWritable chainVertexId) {
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.chainVertexId.set(chainVertexId);
        }
    }
    
    public AdjacencyListWritable getNeighberNode() {
        return neighberNode;
    }

    public void setNeighberNode(AdjacencyListWritable neighberNode) {
        if(neighberNode != null){
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(neighberNode);
        }
    }
    
    public int getLengthOfChain() {
        return chainVertexId.getKmerLength();
    }
    
    public byte getAdjMessage() {
        return adjMessage;
    }

    public void setAdjMessage(byte adjMessage) {
        checkMessage |= CheckMessage.ADJMSG;
        this.adjMessage = adjMessage;
    }

    public byte getMessage() {
        return message;
    }

    public void setMessage(byte message) {
        this.message = message;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(checkMessage);
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.write(out);
        if ((checkMessage & CheckMessage.CHAIN) != 0)
            chainVertexId.write(out);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.write(out);
        if ((checkMessage & CheckMessage.ADJMSG) != 0)
            out.write(adjMessage);
        out.write(message);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.reset();
        checkMessage = in.readByte();
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.readFields(in);
        if ((checkMessage & CheckMessage.CHAIN) != 0)
            chainVertexId.readFields(in);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.readFields(in);
        if ((checkMessage & CheckMessage.ADJMSG) != 0)
            adjMessage = in.readByte();
        message = in.readByte();
    }

    @Override
    public int hashCode() {
        return sourceVertexId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MessageWritable) {
            MessageWritable tp = (MessageWritable) o;
            return sourceVertexId.equals(tp.sourceVertexId);
        }
        return false;
    }

    @Override
    public String toString() {
        return sourceVertexId.toString();
    }

    @Override
    public int compareTo(MessageWritable tp) {
        return sourceVertexId.compareTo(tp.sourceVertexId);
    }
}
