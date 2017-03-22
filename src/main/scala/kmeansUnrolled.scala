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
import scala.collection.mutable.ListBuffer
import scala.util.Random

object kmeansUnrolled {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/Users/jonas/tmp_kmeans_big.txt"
    val k = 10
    val iterations = 4
    val epsilon = 0.01

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
      }
      .withName("Create points")

    // Create initial centroids.
    val random = new Random
    val initialCentroids = planBuilder
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextFloat(), random.nextFloat(), i, 0, false))
      .withName("Load random centroids")
//      .map { x => println("### initial centroid in iteration ZERO: " + x ); x }


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
      .reduceByKey(_.cluster, _.add_points(_))
      .withName("Add up points - iteration zero")
      .withCardinalityEstimator(k)
      .map(_.average)
      .withName("Average points - iteration zero")
//      .map { x => println("### new centroid in iteration ZERO: " + x ); x }

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

    var StableCentroids = mapPartitionOperator_Zero
      .filter(_.stable == true)
      .withName("Filter stable centroids - iteration zero")
//      .map { x => println("### stable centroid in iteration ZERO: " + x ); x }

    // OPERATOR: Filter - unstable // return only the centroids that still change and should be kept
    // input ID_5
    // broadcast_out ID_b0
    // broadcast_out ID_b1
    // broadcast_out ID_b2

    // Use ListBuffers to add operators in every loop iteration

    var UnstableCentroids = new ListBuffer[DataQuanta[TaggedPointCounter]]()

    UnstableCentroids += mapPartitionOperator_Zero
      .filter(_.stable == false)
      .withName("Filter unstable centroids - iteration zero")
//      .map { x => println("### unstable centroid in iteration ZERO: " + x ); x }

    // OPERATOR: Filter - filters out the points belonging to a stable centroid
    // input ID_1
    // broadcast_in ID_b0
    // output ID_6 (remaining points)

    var UnstablePoints = new ListBuffer[DataQuanta[TaggedPointCounter]]()
    UnstablePoints += selectNearestOperator_Zero
      .mapJava(new TagStablePoints)
      .withBroadcast(UnstableCentroids.last, "new_centroids")
      .filter(_.stable == false)


    // END iteration ZERO
    // START iteration 1..n

    for (iteration <- 0 to iterations) {

      // OPERATOR: select nearest centroid
      // input ID_6
      // broadcast_in ID_b1
      // output ID_7

      var selectNearestOperator = UnstablePoints.last
        .mapJava(new SelectNearestCentroidForPoint)
        .withBroadcast(UnstableCentroids.last, "centroids")
        .withName("Find nearest centroid")
//        .map { x => println("### find nearest in iteration " + iteration + ": " + x ); x }

      // OPERATOR: Reduce, Average
      // input ID_7
      // output ID_8

      var reduceAverage = selectNearestOperator
        .reduceByKey(_.cluster, _.add_points(_))
        .withName("Add up points")
        .withCardinalityEstimator(k)
        .map(_.average)
        .withName("Average points")

      // OPERATOR: Group
      // input ID_8
      // output ID_9

      // unclear what for

      // OPERATOR: MapPartition
      // input ID_9
      // broadcast_in ID_b2
      // output ID_10

      var mapPartitionOperator = reduceAverage
        .mapJava(new TagStableCentroids)
        .withBroadcast(UnstableCentroids.last, "centroids")
        .withName("Tag stable centroids")
//        .map { x => println("### TaggedCentroid in iteration " + iteration + ": " + x ); x }

      // OPERATOR: Filter
      // input ID_10
      // output ID_12

      var NewStableCentroids = mapPartitionOperator
        .filter(_.stable == true)
        .withName("Filter stable centroids")
//        .map { x => println("### Stable centroids in iteration " + iteration + ": " + x ); x }


      // OPERATOR: UNION
      // input ID_11
      // input ID_12

      StableCentroids = StableCentroids.union(NewStableCentroids)

      UnstableCentroids += mapPartitionOperator
        .filter(_.stable == false)
        .withName("Filter unstable centroids")
//        .map { x => println("### unstable centroid in iteration " + iteration + ": " + x ); x }

      UnstablePoints += selectNearestOperator
        .mapJava(new TagStablePoints)
        .withBroadcast(UnstableCentroids.last, "new_centroids")
        .filter(_.stable == false)

      // END iteration 1..n
    }


    println("StableCentroids: " + StableCentroids.count.collect())

    // Output of the Unions goes into Collection Sink


  }
}