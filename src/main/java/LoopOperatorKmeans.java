/**
 * Created by jonas on 2/13/17.
 */

import org.qcri.rheem.api.DataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import static java.util.Collections.singletonList;

public class LoopOperatorKmeans {

    public static void main(String[] args){

        // Settings
        String inputUrl = "file:/Users/jonas/tmp_kmeans_big.txt";
        int k = 10;
        final int numIterations = 10;

        // input points
        // input centroids

        // START iteration ZERO

        // OPERATOR: select nearest centroid
        // input points
        // broadcast_in centroids
        // output ID_0
        // output ID_1

        // OPERATOR: Reduce, Average
        // input ID_0
        // output ID_2

        // OPERATOR: Group
        // input ID_2
        // output ID_3

        // OPERATOR: MapPartition // finds out if new centroids are stable or not
        // input ID_3
        // broadcast_in centroids
        // output ID_4
        // output ID_5

        // OPERATOR: Filter - stable // return only the centroids that do not change anymore
        // input ID_4
        // output ID_11

        // OPERATOR: Filter - unstable // return only the centroids that still change and should be kept
        // input ID_5
        // broadcast_out ID_b0
        // broadcast_out ID_b1
        // broadcast_out ID_b2



        // OPERATOR: Filter - filters out the points belonging to a stable centroid
        // input ID_1
        // broadcast_in ID_b0
        // output ID_6 (remaining points)

        // END iteration ZERO
        // START iteration 1..n

        // OPERATOR: select nearest centroid
        // input ID_6
        // broadcast_in ID_b1
        // output ID_7

        // OPERATOR: Reduce, Average
        // input ID_7
        // output ID_8

        // OPERATOR: Group
        // input ID_8
        // output ID_9

        // OPERATOR: MapPartition
        // input ID_9
        // broadcast_in ID_b2
        // output ID_10

        // OPERATOR: Filter
        // input ID_10
        // output ID_12

        // OPERATOR: UNION
        // input ID_11
        // input ID_12

        // END iteration 1..n

        // Output of the Unions goes into Collection Sink


    }
}