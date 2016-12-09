package vertex.data;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FWVertexMessage implements Writable{

    private Long id;
    private List<Long> weights; // weights[i] - value of edge between this vertex and i-th vertex

    public void write(DataOutput dataOutput) throws IOException {
        int size = weights.size();
        dataOutput.write(size);
        for (Long l : weights) {
            dataOutput.writeLong(l);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        weights = new ArrayList<Long>();
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            weights.add(dataInput.readLong());
        }
    }

    public FWVertexMessage() {
    }

    public FWVertexMessage(Long id, List<Long> weights) {
        this.id = id;
        this.weights = weights;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<Long> getWeights() {
        return weights;
    }

    public void setWeights(List<Long> weights) {
        this.weights = weights;
    }
}
