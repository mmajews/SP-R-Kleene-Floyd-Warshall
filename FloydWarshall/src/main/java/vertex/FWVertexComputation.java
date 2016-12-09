package vertex;


import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vertex.data.FWVertexMessage;

import java.io.IOException;

public class FWVertexComputation extends BasicComputation<IntWritable, FWVertexMessage, LongWritable, FWVertexMessage> {

    private static final LongConfOption SOURCE_ID = new LongConfOption("SimpleShortestPathsVertex.sourceId", 1, null);

    private static final Logger LOG = LoggerFactory.getLogger(FWVertexComputation.class);

    private long [][] shortestPaths = new long [(int) getTotalNumVertices()][(int) getTotalNumVertices()];

    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }


    public void compute(Vertex<IntWritable, FWVertexMessage, LongWritable> vertex, Iterable<FWVertexMessage> messages) throws IOException {

        long middleVertexId = getSuperstep();

        if (middleVertexId == 0) {

            int vertexId = vertex.getId().get();
            for (int i = 0; i < (int) getTotalNumVertices(); i++) {
                shortestPaths[vertexId][i] = Long.MAX_VALUE;
            }
            shortestPaths[vertexId][vertexId] = 0;
            for (Edge<IntWritable, LongWritable> edge : vertex.getEdges()) {
                shortestPaths[vertexId][edge.getTargetVertexId().get()] = edge.getValue().get();
            }

        } else {

            Vertex<IntWritable, FWVertexMessage, LongWritable> middleVertex = getWorkerForVertex(new IntWritable((int) middleVertexId));

            for (FWVertexMessage message : messages ) {



            }

        }



        vertex.voteToHalt();
    }
}
