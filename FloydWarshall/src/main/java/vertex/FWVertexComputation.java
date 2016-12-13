package vertex;


import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FWVertexComputation extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    private static final LongConfOption SOURCE_ID = new LongConfOption("SimpleShortestPathsVertex.sourceId", 1, null);

    private static final Logger LOG = LoggerFactory.getLogger(FWVertexComputation.class);

    private static long[][] shortestPaths;

    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
        long middleVertexId = getSuperstep();

        int vertexId = (int) vertex.getId().get();

        int graphSize = (int) getTotalNumVertices();

        if (middleVertexId == 0) {

            initPaths(graphSize);

            for (int i = 0; i < graphSize; i++) {
                shortestPaths[vertexId][i] = Long.MAX_VALUE;
            }
            shortestPaths[vertexId][vertexId] = 0;
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                shortestPaths[vertexId][(int) edge.getTargetVertexId().get()] = (long) edge.getValue().get();
            }

        } else {

            for (DoubleWritable message : messages) {

                int destVertexId = (int) message.get();
                long sum = shortestPaths[vertexId][(int) middleVertexId - 1] + shortestPaths[(int) middleVertexId - 1][destVertexId];
                LOG.debug("i : " + vertexId + " k : " + (middleVertexId - 1) + " j : " + destVertexId);
                if (sum < shortestPaths[vertexId][destVertexId]) {
                    shortestPaths[vertexId][destVertexId] = sum;
                }

            }
        }

        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
            sendMessage(edge.getTargetVertexId(), new DoubleWritable(vertexId));
        }
        if (graphSize == middleVertexId) {
            vertex.voteToHalt();
        }
    }

    private static synchronized void initPaths(int size) {
        if (shortestPaths == null) {
            shortestPaths = new long[size][size];
        }
    }
}
