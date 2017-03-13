/**
 * Created by jonas on 2/13/17.
 */

import org.qcri.rheem.api.DataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.*;

public class DoWhileJava {


    public static void main(String[] args){

        // Settings
        String inputUrl = "file:/Users/jonas/tmp.txt";

        // Get a plan builder.
        RheemContext rheemContext = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(DoWhileJava.class);

        int accuracy = 1;
        int max_iterations = 1;

        ArrayList<String> al = new ArrayList<String>();
        //add elements to the ArrayList
        al.add("foo");
        al.add("bar");

        ArrayList<Integer> ali = new ArrayList<Integer>();
        //add elements to the ArrayList
        ali.add(23);
        ali.add(42);

        // Start building the RheemPlan.
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                .loadCollection(al)
                .doWhile(new LoopCondition(accuracy, max_iterations), w -> {

                    DataQuantaBuilder<?, String> newWeightsDataset = planBuilder.loadCollection(al);

                    DataQuantaBuilder<?, Integer> convergenceDataset = planBuilder.loadCollection(ali);

                    return new Tuple<>(newWeightsDataset, convergenceDataset);

                });

        System.out.println(wordcounts);
    }
}

class LoopCondition implements FunctionDescriptor.ExtendedSerializablePredicate<Collection<Tuple2<Double, Double>>> {

    public double accuracy;
    public int max_iterations;

    private int current_iteration;

    public LoopCondition(double accuracy, int max_iterations) {
        this.accuracy = accuracy;
        this.max_iterations = max_iterations;
    }

    @Override
    public boolean test(Collection<Tuple2<Double, Double>> collection) {
        Tuple2<Double, Double> input = RheemCollections.getSingle(collection);
        System.out.println("Running iteration: " + current_iteration);
        return (input.field0 < accuracy * Math.max(input.field1, 1.0) || current_iteration > max_iterations);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.current_iteration = executionContext.getCurrentIteration();
    }
}