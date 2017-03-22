/**
  * Created by jonas on 2/13/17.
  */
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConversions._
import scala.util.Random

object kmeansUnrolled {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/Users/jonas/tmp_kmeans.txt"
    val k = 5
    val iterations = 10
    val epsilon = 0.000001

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName(s"k-means ($inputUrl, k=$k, $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    case class TaggedPointCounter(x: Double, y: Double, cluster: Int, count: Long, stable: Boolean) {
      def add_points(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count, false)

      def average = TaggedPointCounter(x / count, y / count, cluster, 0, false)
    }

    // Declare UDF to select centroid for each data point.
    class SelectNearestCentroidForPoint extends ExtendedSerializableFunction[TaggedPointCounter, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(point: TaggedPointCounter): TaggedPointCounter = {
        var closest_centroid = -1
        var minDistance = Double.PositiveInfinity
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance) {
            minDistance = distance
            closest_centroid = centroid.cluster
          }
        }
        return new TaggedPointCounter(point.x, point.y, closest_centroid, 1, false)
      }
    }

    // Declare UDF to select centroid for each data point.
    class TagStableCentroids extends ExtendedSerializableFunction[TaggedPointCounter, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(point: TaggedPointCounter): TaggedPointCounter = {
        var minDistance = Double.PositiveInfinity
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance) {
            minDistance = distance
          }
        }
        if (minDistance < epsilon){
          return new TaggedPointCounter(point.x, point.y, point.cluster, 1, true)
        }
        else {
          return new TaggedPointCounter(point.x, point.y, point.cluster, 1, false)
        }

      }
    }

    // Declare UDF to select centroid for each data point.
    class TagStablePoints extends ExtendedSerializableFunction[TaggedPointCounter, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var new_centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        new_centroids = executionCtx.getBroadcast[TaggedPointCounter]("new_centroids")
      }

      override def apply(point: TaggedPointCounter): TaggedPointCounter = {
        var closest_centroid = -1
        var hasCentroidLeft = false
        for (centroid <- new_centroids) {
          if (centroid.cluster == point.cluster){
            hasCentroidLeft = true
          }
        }
        if (hasCentroidLeft){
          return new TaggedPointCounter(point.x, point.y, point.cluster, 1, false)
        }
        else {
          return new TaggedPointCounter(point.x, point.y, point.cluster, 1, true)
        }

      }
    }


    // input points
    // input centroids


    // Read and parse the input file(s).
    val points = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 0, false)
      }.withName("Create points")

    // Create initial centroids.
    val random = new Random
    val initialCentroids = planBuilder
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextFloat(), random.nextFloat(), i, 0, false))
      .withName("Load random centroids")


    // START iteration ZERO

    // OPERATOR: select nearest centroid
    // input points
    // broadcast_in centroids
    // output ID_0
    // output ID_1

    var selectNearestOperator_Zero = points
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(initialCentroids, "centroids")
      .withName("Find nearest centroid - iteration zero")

    // OPERATOR: Reduce, Average
    // input ID_0
    // output ID_2

    var reduceAverage_Zero = selectNearestOperator_Zero
      .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points - iteration zero")
      .withCardinalityEstimator(k)
      .map(_.average)
      .withName("Average points - iteration zero")

    // OPERATOR: Group
    // input ID_2
    // output ID_3

    // unclear what for

    // OPERATOR: MapPartition // finds out if new centroids are stable or not
    // input ID_3
    // broadcast_in centroids
    // output ID_4
    // output ID_5

    var mapPartitionOperator_Zero = reduceAverage_Zero
      .mapJava(new TagStableCentroids)
      .withBroadcast(initialCentroids, "centroids")
      .withName("Tag stable centroids - iteration zero")

    // OPERATOR: Filter - stable // return only the centroids that do not change anymore
    // input ID_4
    // output ID_11

    var FilterStableOperator_Zero = mapPartitionOperator_Zero
      .filter(_.stable == true)
      .withName("Filter stable centroids - iteration zero")

    // OPERATOR: Filter - unstable // return only the centroids that still change and should be kept
    // input ID_5
    // broadcast_out ID_b0
    // broadcast_out ID_b1
    // broadcast_out ID_b2

    var FilterUnstableOperator_Zero = mapPartitionOperator_Zero
      .filter(_.stable == false)
      .withName("Filter unstable centroids - iteration zero")

    // OPERATOR: Filter - filters out the points belonging to a stable centroid
    // input ID_1
    // broadcast_in ID_b0
    // output ID_6 (remaining points)

    var FilterPointsOperator_Zero = selectNearestOperator_Zero
        .mapJava(new TagStablePoints)
        .withBroadcast(FilterUnstableOperator_Zero, "new_centroids")
        .filter(_.stable == false)

    println(FilterPointsOperator_Zero.collect())

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