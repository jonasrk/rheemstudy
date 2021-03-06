/**
 * Created by jonas on 2/13/17.
 */

import org.qcri.rheem.api.*;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.*;

import static java.util.Collections.*;

class TaggedPointCounter{

    double x;
    double y;
    int cluster;
    long count;

    public TaggedPointCounter(double x, double y, int cluster, long count)
    {
        this.x = x;
        this.y = y;
        this.cluster = cluster;
        this.count = count;
    }
    public TaggedPointCounter add_points(TaggedPointCounter that)
    {
        return new TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count);
    }
    public TaggedPointCounter average(){
        return new TaggedPointCounter(this.x / this.count, this.y / this.count, this.cluster, 0);
    }
}

public class loopoperator {

    private static Collection<TaggedPointCounter> generate_random_centroids(int n_points){
        Random rand = new Random();
        Collection<TaggedPointCounter> list = new ArrayList<>();
        for (int i = 0; i < n_points; i++) {

            list.add(new TaggedPointCounter(rand.nextDouble(), rand.nextDouble(), i, 0));
        }
        return list;
    }

    public static void main(String[] args){

        // Settings
        String inputUrl = "file:/Users/jonas/tmp_kmeans_big.txt";
        int k = 10;
        final int numIterations = 10;

        // Get a plan builder.
        RheemContext rheemContext = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext)
                .withJobName("k-means ($inputUrl, k=$k, $iterations iterations)")
                .withUdfJarOf(loopoperator.class);


        // Start building the RheemPlan.
        final DataQuantaBuilder<?, TaggedPointCounter> points = planBuilder
                .readTextFile(inputUrl).withName("Load file")
                .flatMap(line -> singletonList(new TaggedPointCounter(
                        Double.parseDouble(line.split(",")[0]),
                        Double.parseDouble(line.split(",")[1]), 0, 0)));

        Collection<TaggedPointCounter> points_collection = points.collect();

        // Create initial centroids.
        Collection<TaggedPointCounter> centroids = generate_random_centroids(k);

//        System.out.println("initial points");
//        for (TaggedPointCounter el:
//                points_collection) {
//            System.out.println("x: " + el.x + " y: " + el.y + " cluster: " + el.cluster + " count: " + el.count);
//        }

        System.out.println("initial centroids");
        for (TaggedPointCounter el:
                centroids) {
            System.out.println("x: " + el.x + " y: " + el.y + " cluster: " + el.cluster + " count: " + el.count);
        }


        CollectionSource<TaggedPointCounter> source = new CollectionSource<>(centroids, TaggedPointCounter.class);
        source.setName("source");

        CollectionSource<Integer> convergenceSource = new CollectionSource<>(RheemArrays.asList(0), Integer.class);
        convergenceSource.setName("convergenceSource");

        LoopOperator<TaggedPointCounter, Integer> loopOperator = new LoopOperator<>(
                DataSetType.createDefault(TaggedPointCounter.class),
                DataSetType.createDefault(Integer.class),
                (PredicateDescriptor.SerializablePredicate<Collection<Integer>>) collection ->
                        collection.iterator().next() >= numIterations,
                numIterations
        );
        loopOperator.setName("loop");
        loopOperator.initialize(source, convergenceSource);


        MapPartitionsOperator<TaggedPointCounter, TaggedPointCounter> readInPointsOperator = new MapPartitionsOperator<>(
                vals -> points_collection,
                TaggedPointCounter.class,
                TaggedPointCounter.class
        );
        readInPointsOperator.setName("read in points");

        MapOperator<TaggedPointCounter, TaggedPointCounter> nearestCentroidOperator;
        nearestCentroidOperator = new MapOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<TaggedPointCounter, TaggedPointCounter>(){
                            /**
                             * Keeps the broadcasted centroids.
                             */
                            Iterable<TaggedPointCounter> centroids;

                            @Override
                            public TaggedPointCounter apply(TaggedPointCounter point) {
                                int closest_centroid = -1;
                                double minDistance = Double.MAX_VALUE;
                                for (TaggedPointCounter centroid : centroids) {
                                    double distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5);
                                    if (distance < minDistance) {
                                        minDistance = distance;
                                        closest_centroid = centroid.cluster;
                                    }
                                }
                                return new TaggedPointCounter(point.x, point.y, closest_centroid, 1);
                            }

                            @Override
                            public void open(ExecutionContext executionCtx) {
                                centroids = executionCtx.getBroadcast("centroids");
                            }
                        },
                        TaggedPointCounter.class,
                        TaggedPointCounter.class
                )
        );
        nearestCentroidOperator.setName("select nearest centroid");

        ReduceByOperator<TaggedPointCounter, Integer> reduceByClusterAndAddOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<TaggedPointCounter, Integer>() {
                            @Override
                            public Integer apply(TaggedPointCounter point) {
                                return point.cluster;
                            }
                            @Override
                            public void open(ExecutionContext executionCtx) {
                            }
                        },
                        TaggedPointCounter.class,
                        Integer.class
                ),
                new ReduceDescriptor<>(
                        (FunctionDescriptor.SerializableBinaryOperator<TaggedPointCounter>) (taggedPointCounter, taggedPointCounter2) -> taggedPointCounter.add_points(taggedPointCounter2),
                        TaggedPointCounter.class
                )
        );
        reduceByClusterAndAddOperator.setName("reduce by cluster and add up");


        MapOperator<TaggedPointCounter, TaggedPointCounter> averageOperator;
        averageOperator = new MapOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<TaggedPointCounter, TaggedPointCounter>(){
                            @Override
                            public TaggedPointCounter apply(TaggedPointCounter point) {
                                return new TaggedPointCounter(point.x / point.count, point.y / point.count, point.cluster, 0);
                            }

                            @Override
                            public void open(ExecutionContext executionCtx) {
                            }
                        },
                        TaggedPointCounter.class,
                        TaggedPointCounter.class
                )
        );
        averageOperator.setName("take average");


        MapOperator<Integer, Integer> counter = new MapOperator<>(
                new TransformationDescriptor<>(n -> n + 1, Integer.class, Integer.class)
        );
        counter.setName("counter");

        loopOperator.beginIteration(readInPointsOperator, counter);
        loopOperator.broadcastTo("iterOut", nearestCentroidOperator, "centroids");
        readInPointsOperator.connectTo(0, nearestCentroidOperator, 0);
        nearestCentroidOperator.connectTo(0, reduceByClusterAndAddOperator, 0);
        reduceByClusterAndAddOperator.connectTo(0, averageOperator, 0);
        loopOperator.endIteration(averageOperator, counter);


        Collection<TaggedPointCounter> output = new ArrayList<>();

        LocalCallbackSink<TaggedPointCounter> sink = LocalCallbackSink.createCollectingSink(output, TaggedPointCounter.class);
        sink.setName("sink");
        loopOperator.outputConnectTo(sink);


        RheemPlan rheemPlan = new RheemPlan(sink);

        rheemContext.execute(rheemPlan);


        System.out.println("final centroids");
        for (TaggedPointCounter el:
                output) {
            System.out.println("x: " + el.x + " y: " + el.y + " cluster: " + el.cluster + " count: " + el.count);
        }
    }
}