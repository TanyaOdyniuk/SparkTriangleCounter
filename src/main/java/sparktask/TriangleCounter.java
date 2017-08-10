package sparktask;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TriangleCounter {
    public static void main(String[] args) {
        final String master = "local[*]";
        final int triangleAngleCount = 6;
        final String splitter = " ";
        final String textPath = "src/main/resources/graph.txt";
        SparkConf conf = new SparkConf().setAppName(TriangleCounter.class.getName()).setMaster(master);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> data = sparkContext.textFile(textPath);

        JavaPairRDD<Integer, Integer> edgePairs = data.mapToPair(s -> {
            String[] vertices = s.split(splitter);
            return new Tuple2<>(Integer.parseInt(vertices[0]), Integer.parseInt(vertices[1]));
        });

        JavaPairRDD<Integer, Integer>  swappedEdgePairs = edgePairs.mapToPair(item -> item.swap());

        JavaPairRDD<Integer,Integer> finalEdgePairs = edgePairs.union(swappedEdgePairs);

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> angles =  finalEdgePairs.join(finalEdgePairs).distinct();

        JavaPairRDD<Tuple2<Integer,Integer>, Integer> swappedAngles = angles.mapToPair(item -> item.swap());

        JavaPairRDD triangles = swappedAngles.join(
                finalEdgePairs.mapToPair(
                        item -> new Tuple2<Tuple2<Integer, Integer>, Integer>(item, null)))
                .distinct();

        System.out.println("Count of triangles is " + triangles.count() / triangleAngleCount);
    }
}