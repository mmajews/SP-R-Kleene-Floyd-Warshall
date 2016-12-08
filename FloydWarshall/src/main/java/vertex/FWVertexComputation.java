package vertex;


import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vertex.data.FWVertexBody;

import java.io.IOException;

public class FWVertexComputation extends BasicComputation<IntWritable, FWVertexBody, LongWritable, FWVertexBody> {

    private static final LongConfOption SOURCE_ID = new LongConfOption("SimpleShortestPathsVertex.sourceId", 1, null);

    private static final Logger LOG = LoggerFactory.getLogger(FWVertexComputation.class);

    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }


    public void compute(Vertex<IntWritable, FWVertexBody, LongWritable> vertex, Iterable<FWVertexBody> iterable) throws IOException {
        vertex.voteToHalt();
    }
}
