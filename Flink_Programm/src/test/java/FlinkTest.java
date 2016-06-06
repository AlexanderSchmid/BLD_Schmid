import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Alex on 06.06.2016.
 */
public class FlinkTest {

    @Test
    public void testFlinkExample() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "Ich heisse Alexander Schmid",
                "Alexander kommt hier ein zweites mal vor",
                "und noch ein Alexander",
                "Alexander vier und Alexander fuenf"
        );

        List<Tuple2<String, Integer>> counts =
                text.flatMap(new Splitter())
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1)
                        .collect();

        System.out.println(Arrays.toString(counts.toArray()));

        List<Tuple2<String, Integer>> filtered = counts.stream().filter(p -> p.f0.contains("Alexander")).collect(Collectors.toList());
        System.out.println(Arrays.toString(filtered.toArray()));

        Assert.assertTrue(filtered.get(0).f1 == 5);
    }
}
