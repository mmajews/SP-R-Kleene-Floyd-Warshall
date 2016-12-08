package vertex.data;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FWVertexBody implements Writable{

    private long[] weights; // weights[i] - value of edge between this vertex and i-th vertex


    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
