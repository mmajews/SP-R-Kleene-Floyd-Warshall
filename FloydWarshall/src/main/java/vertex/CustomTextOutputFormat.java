package vertex;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class CustomTextOutputFormat<I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends TextVertexOutputFormat<I, V, E> {

    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public static final String REVERSE_ID_AND_VALUE = "reverse.id.and.value";

    public static final boolean REVERSE_ID_AND_VALUE_DEFAULT = false;

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new IdWithValueVertexWriter();
    }

    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {

        private String delimiter;
        private boolean reverseOutput;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(
                    LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            reverseOutput = getConf().getBoolean(
                    REVERSE_ID_AND_VALUE, REVERSE_ID_AND_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<I, V, E> vertex)
                throws IOException {

            StringBuilder outputTable = new StringBuilder();
            final int length = FWVertexComputation.shortestPaths.length;
            for (int i = 0; i < length; i++) {
                for (int j = 0; j < FWVertexComputation.shortestPaths[i].length; j++) {
                    outputTable.append(i).append(",").append(j).append(":").append(FWVertexComputation.shortestPaths[i][j]);
                }
            }

            return new Text(outputTable.toString());
        }
    }
}

