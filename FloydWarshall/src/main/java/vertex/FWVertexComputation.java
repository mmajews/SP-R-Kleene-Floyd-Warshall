package vertex;


import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FWVertexComputation extends BasicComputation<IntWritable, LongWritable, LongWritable, LongWritable> {

    private static final LongConfOption SOURCE_ID = new LongConfOption("SimpleShortestPathsVertex.sourceId", 1, null);

    private static final Logger LOG = LoggerFactory.getLogger(FWVertexComputation.class);

    private long [][] shortestPaths = new long [(int) getTotalNumVertices()][(int) getTotalNumVertices()];

    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(Vertex<IntWritable, LongWritable, LongWritable> vertex, Iterable<LongWritable> messages) throws IOException {

        long middleVertexId = getSuperstep();

        int vertexId = vertex.getId().get();

        if (middleVertexId == 0) {

            for (int i = 0; i < (int) getTotalNumVertices(); i++) {
                shortestPaths[vertexId][i] = Long.MAX_VALUE;
            }
            shortestPaths[vertexId][vertexId] = 0;
            for (Edge<IntWritable, LongWritable> edge : vertex.getEdges()) {
                shortestPaths[vertexId][edge.getTargetVertexId().get()] = edge.getValue().get();
            }

        } else {

            for (LongWritable message : messages ) {

                int destVertexId = (int) message.get();
                long sum = shortestPaths[vertexId][(int) middleVertexId - 1] + shortestPaths[(int) middleVertexId - 1][destVertexId];
                LOG.debug("i : " + vertexId + " k : " + middleVertexId + " j : " + destVertexId);
                if (sum < shortestPaths[vertexId][ destVertexId]) {
                    shortestPaths[vertexId][destVertexId] = sum;
                }

            }
        }

        for (Edge<IntWritable, LongWritable> edge : vertex.getEdges()) {
            sendMessage(edge.getTargetVertexId(), new LongWritable(vertexId));
        }
        vertex.voteToHalt();
    }
}
