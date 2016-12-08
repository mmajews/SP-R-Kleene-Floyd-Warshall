package vertex.data;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FWVertexBody implements Writable{

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
}
