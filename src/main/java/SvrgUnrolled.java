import org.qcri.rheem.api.DataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.net.MalformedURLException;
import java.util.*;

/**
 * This class executes a stochastic variance reduced gradient optimization on Rheem.
 */
public class SvrgUnrolled {

    // Default parameters.
    static String relativePath = "svrg/src/main/resources/adult.zeros";
    static int datasetSize  = 100827;
    static int features = 123;

    //these are for SVRG/mini run to convergence
    static int sampleSize = 10;
    static double accuracy = 0.001;
    static int max_iterations = 1000;
    static int iterations = 500; // so far 650 was maximum

    static Platform full_iteration_platform, partial_iteration_platform;

    public static void main (String... args) throws MalformedURLException {

        //Usage: <data_file> <#features> <sparse> <binary>
        if (args.length > 0) {
            relativePath = args[0];
            datasetSize = Integer.parseInt(args[1]);
            features = Integer.parseInt(args[2]);
            max_iterations = Integer.parseInt(args[3]);
            accuracy = Double.parseDouble(args[4]);
            sampleSize = Integer.parseInt(args[5]);
            if (args[6].equals("all_spark")){
                full_iteration_platform = Spark.platform();
                partial_iteration_platform = Spark.platform();
            } else if (args[6].equals("all_java")){
                full_iteration_platform = Java.platform();
                partial_iteration_platform = Java.platform();
            } else if (args[6].equals("mixed")){
                full_iteration_platform = Spark.platform();
                partial_iteration_platform = Java.platform();
            }
            iterations = Integer.parseInt(args[7]);
        }
        else {
            System.out.println("Usage: java <main class> [<dataset path> <dataset size> <#features> <max iterations> <accuracy> <sample size> <platform:all_spark|all_java|mixed> <iterations>]");
            System.out.println("Loading default values");
        }

        String file = relativePath;

        System.out.println("max #iterations:" + max_iterations);
        System.out.println("accuracy:" + accuracy);
        System.out.println("iterations:" + iterations);
        System.out.println("full_iteration_platform:" + full_iteration_platform);
        System.out.println("partial_iteration_platform:" + partial_iteration_platform);

        new SvrgUnrolled().execute(file, features);
    }

    public void execute(String fileName, int features) {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext)
                .withUdfJarOf(WeightsUpdateFullIteration.class)
                .withUdfJarOf(Sum.class)
                .withUdfJarOf(WeightsUpdate.class)
                .withUdfJarOf(ComputeLogisticGradient.class)
                .withUdfJarOf(ComputeLogisticGradientFullIteration.class)
                .withUdfJarOf(Transform.class)
                .withUdfJarOf(SvrgUnrolled.class)
                .withUdfJarOf(this.getClass());

        List<double[]> weights = Arrays.asList(new double[features]);
        final DataQuantaBuilder<?, double[]> weightsBuilder = javaPlanBuilder
                .loadCollection(weights)
                .withTargetPlatform(partial_iteration_platform)
                .withName("init weights");

        final DataQuantaBuilder<?, double[]> transformBuilder = javaPlanBuilder
                .readTextFile(fileName).withName("source")
                .withTargetPlatform(partial_iteration_platform)
                .map(new Transform(features)).withName("transform")
                .withTargetPlatform(partial_iteration_platform); // TODO JRK Double Check which platform should do this


        // START iteration ZERO

        List<Integer> current_iteration = Arrays.asList(0);

        DataQuantaBuilder<?, Integer> iteration_list = javaPlanBuilder
                .loadCollection(current_iteration)
                .withTargetPlatform(full_iteration_platform);

        // Operator Lists:
        ArrayList<DataQuantaBuilder<?, double[]>> FullOperatorList = new ArrayList<>();
        ArrayList<DataQuantaBuilder<?, double[]>> muOperatorList = new ArrayList<>();
        ArrayList<DataQuantaBuilder<?, double[]>> PartialOperatorList = new ArrayList<>();

        muOperatorList.add(
                transformBuilder
                        .map(new ComputeLogisticGradientFullIteration())
                        .withBroadcast(weightsBuilder, "weights")
                        .withTargetPlatform(full_iteration_platform)
                        .withName("compute")

                        .reduce(new Sum()).withName("reduce")
                        .withTargetPlatform(full_iteration_platform)
        );

        FullOperatorList.add(
                muOperatorList.get(muOperatorList.size() - 1)
                        .map(new WeightsUpdateFullIteration())
                        .withBroadcast(weightsBuilder, "weights")
                        .withBroadcast(iteration_list, "current_iteration")
                        .withTargetPlatform(full_iteration_platform)
                        .withName("update")
        );

        PartialOperatorList.add(
                muOperatorList.get(muOperatorList.size() - 1)
                        .map(new WeightsUpdateFullIteration())
                        .withBroadcast(weightsBuilder, "weights")
                        .withBroadcast(iteration_list, "current_iteration")
                        .withTargetPlatform(full_iteration_platform)
                        .withName("update")
        );

        // END iteration ZERO


        // START other iterations

        for (int i = 1; i < iterations; i++) {

            if (i % 3 == 0){

                FullOperatorList.add(muOperatorList.get(muOperatorList.size() - 1)
                        .map(new WeightsUpdateFullIteration())
                        .withBroadcast(PartialOperatorList.get(PartialOperatorList.size() - 1), "weights")
                        .withBroadcast(iteration_list, "current_iteration")
                        .withTargetPlatform(full_iteration_platform)
                        .withName("update"));

                current_iteration = Arrays.asList(i);

                iteration_list = javaPlanBuilder
                        .loadCollection(current_iteration)
                        .withTargetPlatform(full_iteration_platform);

                muOperatorList.add(transformBuilder
                        .map(new ComputeLogisticGradientFullIteration())
                        .withBroadcast(PartialOperatorList.get(PartialOperatorList.size() - 1), "weights")
                        .withTargetPlatform(full_iteration_platform)
                        .withName("compute")

                        .reduce(new Sum()).withName("reduce")
                        .withTargetPlatform(full_iteration_platform)); // returns the gradientBar from the full iteration for all training examples

            } else { // partial iteration

                current_iteration = Arrays.asList(i);

                iteration_list = javaPlanBuilder
                        .loadCollection(current_iteration)
                        .withTargetPlatform(partial_iteration_platform);

                PartialOperatorList.add(transformBuilder
                        .sample(1)
                        .withTargetPlatform(partial_iteration_platform)

                        .map(new ComputeLogisticGradient())
                        .withBroadcast(FullOperatorList.get(FullOperatorList.size() - 1), "weightsBar")
                        .withBroadcast(PartialOperatorList.get(PartialOperatorList.size() - 1), "weights")
                        .withTargetPlatform(partial_iteration_platform)
                        .withName("compute") // returns both grad and gradBar in a single array

                        .map(new WeightsUpdate())
                        .withBroadcast(muOperatorList.get(muOperatorList.size() - 1), "mu")
                        .withBroadcast(PartialOperatorList.get(PartialOperatorList.size() - 1), "weights")
                        .withBroadcast(iteration_list, "current_iteration")
                        .withTargetPlatform(partial_iteration_platform)
                        .withName("update"));
            }
        }
        // END other iterations

        System.out.println("Output weights:" + Arrays.toString(RheemCollections.getSingle(FullOperatorList.get(FullOperatorList.size() - 1).collect())));
    }
}

class Transform implements FunctionDescriptor.SerializableFunction<String, double[]> {

    int features;

    public Transform (int features) {
        this.features = features;
    }

    @Override
    public double[] apply(String line) {
        String[] pointStr = line.split(" ");
        double[] point = new double[features+1];
        point[0] = Double.parseDouble(pointStr[0]);
        for (int i = 1; i < pointStr.length; i++) {
            if (pointStr[i].equals("")) {
                continue;
            }
            String kv[] = pointStr[i].split(":", 2);
            point[Integer.parseInt(kv[0])-1] = Double.parseDouble(kv[1]);
        }
        return point;
    }
}

class ComputeLogisticGradientFullIteration implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights;

    @Override
    public double[] apply(double[] point) {
        double[] gradient = new double[point.length];
        double dot = 0;
        for (int j = 0; j < weights.length; j++)
            dot += weights[j] * point[j + 1];

        for (int j = 0; j < weights.length; j++)
            gradient[j + 1] = ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

        gradient[0] = 1; //counter for the step size required in the update
        return gradient;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
    }
}

class ComputeLogisticGradient implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights, weightsBar;

    double[] calculateGradient(double[] weights, double[] point){
        double[] gradient = new double[point.length];
        double dot = 0;
        for (int j = 0; j < weights.length; j++)
            dot += weights[j] * point[j + 1];

        for (int j = 0; j < weights.length; j++)
            gradient[j + 1] = ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

        gradient[0] = 1; //counter for the step size required in the update
        return gradient;
    }

    @Override
    public double[] apply(double[] point) {
        double[] sumGrad = calculateGradient(weights, point);
        double[] sumGradBar = calculateGradient(weightsBar, point);
        double[] mergedGradients = mergeArrays(sumGrad, sumGradBar);
        return mergedGradients;
    }

    private static double[] mergeArrays(double[] a, double[] b) {
        int aLen = a.length;
        int bLen = b.length;
        double[] merged = new double[aLen + bLen];
        System.arraycopy(a, 0, merged, 0, aLen);
        System.arraycopy(b, 0, merged, aLen, bLen);
        return merged;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        this.weightsBar = (double[]) executionContext.getBroadcast("weightsBar").iterator().next();
    }
}

class Sum implements FunctionDescriptor.SerializableBinaryOperator<double[]> {

    @Override
    public double[] apply(double[] o, double[] o2) {
        double[] g1 = o;
        double[] g2 = o2;

        if (g2 == null) //samples came from one partition only
            return g1;

        if (g1 == null) //samples came from one partition only
            return g2;

        double[] sum = new double[g1.length];
        sum[0] = g1[0] + g2[0]; //count
        for (int i = 1; i < g1.length; i++)
            sum[i] = g1[i] + g2[i];

        return sum;
    }
}

class WeightsUpdate implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights;
    double[] mu;
    int current_iteration;
    double lambda = 0;

    double stepSize = 1;
    double regulizer = 0;

    public WeightsUpdate () { }

    public WeightsUpdate (double stepSize, double regulizer) {
        this.stepSize = stepSize;
        this.regulizer = regulizer;
    }

    @Override
    public double[] apply(double[] input) {

        double count = 100000;//input[0]; // TODO JRK Do not hardcode
        double alpha = (stepSize / (current_iteration+1));

        double[] newWeights = new double[weights.length];
        for (int j = 0; j < weights.length; j++) {

//            double regulizer_term = (1 - alpha * regulizer);
            double old_weight_term = weights[j];
//            double step_size_term = alpha * (1.0 / count);
//            double gradient_term = input[j + 1];
//            newWeights[j] = regulizer_term * old_weight_term - step_size_term * gradient_term;

            double svrg_gradient_term =  input[j + 1] - input[weights.length + j + 2] + (1.0/count) * mu[j];
            double svrg_regulizer_term = lambda*alpha*weights[j];
            double svrg_stepsize_term = alpha;
            newWeights[j] = old_weight_term - svrg_stepsize_term * svrg_gradient_term + svrg_regulizer_term;
        }
        return newWeights;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        this.mu = (double[]) executionContext.getBroadcast("mu").iterator().next();
        this.current_iteration = ((Integer) executionContext.getBroadcast("current_iteration").iterator().next());
    }
}

class WeightsUpdateFullIteration implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights;
    int current_iteration;

    double stepSize = 1;
    double regulizer = 0;

    public WeightsUpdateFullIteration () { }

    public WeightsUpdateFullIteration (double stepSize, double regulizer) {
        this.stepSize = stepSize;
        this.regulizer = regulizer;
    }

    @Override
    public double[] apply(double[] input) {

        double count = input[0];
        double alpha = (stepSize / (current_iteration+1));

        double[] newWeights = new double[weights.length];
        for (int j = 0; j < weights.length; j++) {
            newWeights[j] = (1 - alpha * regulizer) * weights[j] - alpha * (1.0 / count) * input[j + 1];
        }
        return newWeights;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        this.current_iteration = ((Integer) executionContext.getBroadcast("current_iteration").iterator().next());
    }
}

//class ComputeNorm implements FunctionDescriptor.ExtendedSerializableFunction<double[], Tuple2<Double, Double>> {
//
//    double[] previousWeights;
//
//    @Override
//    public Tuple2<Double, Double> apply(double[] weights) {
//        double normDiff = 0.0;
//        double normWeights = 0.0;
//        for (int j = 0; j < weights.length; j++) {
////            normDiff += Math.sqrt(Math.pow(Math.abs(weights[j] - input[j]), 2));
//            normDiff += Math.abs(weights[j] - previousWeights[j]);
////            normWeights += Math.sqrt(Math.pow(Math.abs(input[j]), 2));
//            normWeights += Math.abs(weights[j]);
//        }
//        return new Tuple2(normDiff, normWeights);
//    }
//
//    @Override
//    public void open(ExecutionContext executionContext) {
//        this.previousWeights = (double[]) executionContext.getBroadcast("weights").iterator().next();
//    }
//}
//
//class LoopCondition implements FunctionDescriptor.ExtendedSerializablePredicate<Collection<Tuple2<Double, Double>>> {
//
//    public double accuracy;
//    public int max_iterations;
//
//    private int current_iteration;
//
//    public LoopCondition(double accuracy, int max_iterations) {
//        this.accuracy = accuracy;
//        this.max_iterations = max_iterations;
//    }
//
//    @Override
//    public boolean test(Collection<Tuple2<Double, Double>> collection) {
//        Tuple2<Double, Double> input = RheemCollections.getSingle(collection);
//        System.out.println("Running iteration: " + current_iteration);
//        return (input.field0 < accuracy * Math.max(input.field1, 1.0) || current_iteration > max_iterations);
//    }
//
//    @Override
//    public void open(ExecutionContext executionContext) {
//        this.current_iteration = executionContext.getCurrentIteration();
//    }
//}