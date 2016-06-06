import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by Alex on 06.06.2016.
 */
public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
        for (String word : line.split(" ")) {
            out.collect(new Tuple2<String, Integer>(word, 1));
        }
    }
}
