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
    var inputUrlPoints, inputUrlCentroids = ""
    var iterations = -1
    var epsilon = -0.1

    val platforms = Array(Java.platform, Spark.platform)
    var first_iteration_platform_id, final_count_platform_id, m = 0

    iterations = args(1).toInt
    epsilon = args(2).toFloat
    inputUrlPoints = args(3)
    inputUrlCentroids = args(4)

    val platform = args(0)
    if (platform.equals("mixed")){
      first_iteration_platform_id = 1
      final_count_platform_id = 0
      m = args(5).toInt
    } else if (platform.equals("spark")) {
      first_iteration_platform_id = 1
      final_count_platform_id = 1
      m = iterations
    } else if (platform.equals("java")) {
      first_iteration_platform_id = 0
      final_count_platform_id = 0
      m = 0
    }

    println("\n\n### platform: " + platform + ", iterations: " + iterations + ", m: " + m + ", epsilon: " + epsilon + ", inputUrl: " + inputUrlPoints + ", inputUrlCentroids: " + inputUrlCentroids + "\n\n")

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName(s"k-means ($inputUrlPoints, $inputUrlCentroids, $platform, m=$m, epsilon=$epsilon , $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    case class TaggedPointCounter(x: Double, y: Double, cluster: Int, count: Long, stable: Boolean) {
      def add_points(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count, false)

      def average = TaggedPointCounter(x / count, y / count, cluster, 0, false)
    }

    case class CountWithIteration(count: Long, iteration: Int) {}

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

    class AnnotateUnstablePointsCounts extends ExtendedSerializableFunction[java.lang.Long, CountWithIteration]  {

      var iteration: Iterable[Int] = _

      override def open(executionCtx: ExecutionContext) = {
        iteration = executionCtx.getBroadcast[Int]("iteration_id")
      }

      override def apply(count: java.lang.Long): CountWithIteration = {
        return new CountWithIteration(count, iteration.last)
      }
    }


    // Read and parse the input file(s).
    val points = planBuilder
      .readTextFile(inputUrlPoints).withName("Read points file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 0, false)
      }
      .withName("Load points from hdfs")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    val random = new Random
    val initialCentroids = planBuilder
      .readTextFile(inputUrlCentroids)
      .withName("Read centroids file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, fields(2).toInt, 0, false)
      }
      .withName("Load centroids from hdfs")
      .withTargetPlatforms(platforms(first_iteration_platform_id))


    // START iteration ZERO

    var selectNearestOperator_Zero = points
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(initialCentroids, "centroids")
      .withName("Find nearest centroid - iteration zero")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    var reduceAverage_Zero = selectNearestOperator_Zero
      .reduceByKey(_.cluster, _.add_points(_))
      .withTargetPlatforms(platforms(first_iteration_platform_id))
      .withName("Add up points - iteration zero")
      .map(_.average)
      .withName("Average points - iteration zero")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    // OPERATOR: Map // finds out if new centroids are stable or not
    var mapPartitionOperator_Zero = reduceAverage_Zero
      .mapJava(new TagStableCentroids)
      .withBroadcast(initialCentroids, "centroids")
      .withName("Tag stable centroids - iteration zero")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    // OPERATOR: Filter - stable // return only the centroids that do not change anymore
    var StableCentroids = mapPartitionOperator_Zero
      .filter(_.stable == true)
      .withName("Filter stable centroids - iteration zero")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    // Use ListBuffers to add operators in every loop iteration
    var UnstableCentroids = new ListBuffer[DataQuanta[TaggedPointCounter]]()
    UnstableCentroids += mapPartitionOperator_Zero
      .filter(_.stable == false)
      .withName("Filter unstable centroids - iteration zero")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    var UnstablePoints = new ListBuffer[DataQuanta[TaggedPointCounter]]()
    UnstablePoints += selectNearestOperator_Zero
      .mapJava(new TagStablePoints)
      .withTargetPlatforms(platforms(first_iteration_platform_id))
      .withBroadcast(UnstableCentroids.last, "new_centroids")
      .filter(_.stable == false)
      .withName("Filter unstable points - iteration zero")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    var iteration_zero_list = planBuilder
      .loadCollection(List(0))
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    var UnstablePointsCount = new ListBuffer[DataQuanta[CountWithIteration]]()
    UnstablePointsCount += UnstablePoints.last
      .count
      .withTargetPlatforms(platforms(first_iteration_platform_id))
      .mapJava(new AnnotateUnstablePointsCounts)
      .withBroadcast(iteration_zero_list, "iteration_id")
      .withTargetPlatforms(platforms(first_iteration_platform_id))

    var AllUnstablePointsCounts = UnstablePointsCount.last



    // END iteration ZERO

    def one_iteration(platform_id: Int, iteration: Int)={

      var selectNearestOperator = UnstablePoints.last
        .mapJava(new SelectNearestCentroidForPoint)
        .withBroadcast(UnstableCentroids.last, "centroids")
        .withName("Find nearest centroid")
        .withTargetPlatforms(platforms(platform_id))

      var reduceAverage = selectNearestOperator
        .reduceByKey(_.cluster, _.add_points(_))
        .withTargetPlatforms(platforms(platform_id))
        .withName("Add up points")
        .map(_.average)
        .withName("Average points")
        .withTargetPlatforms(platforms(platform_id))

      var mapPartitionOperator = reduceAverage
        .mapJava(new TagStableCentroids)
        .withBroadcast(UnstableCentroids.last, "centroids")
        .withName("Tag stable centroids")
        .withTargetPlatforms(platforms(platform_id))

      var NewStableCentroids = mapPartitionOperator
        .filter(_.stable == true)
        .withName("Filter stable centroids")
        .withTargetPlatforms(platforms(platform_id))

      StableCentroids = StableCentroids
        .union(NewStableCentroids)
        .withName("union")
        .withTargetPlatforms(platforms(platform_id))

      UnstableCentroids += mapPartitionOperator
        .filter(_.stable == false)
        .withName("Filter unstable centroids")
        .withTargetPlatforms(platforms(platform_id))

      UnstablePoints += selectNearestOperator
        .mapJava(new TagStablePoints)
        .withTargetPlatforms(platforms(platform_id))
        .withBroadcast(UnstableCentroids.last, "new_centroids")
        .filter(_.stable == false)
        .withName("Filter unstable points")
        .withTargetPlatforms(platforms(platform_id))

      var iteration_list = planBuilder
        .loadCollection(List(iteration))
        .withTargetPlatforms(platforms(platform_id))

      UnstablePointsCount += UnstablePoints.last
        .count
        .withTargetPlatforms(platforms(platform_id))
        .mapJava(new AnnotateUnstablePointsCounts)
        .withBroadcast(iteration_list, "iteration_id")
        .withTargetPlatforms(platforms(platform_id))

      AllUnstablePointsCounts = AllUnstablePointsCounts
        .union(UnstablePointsCount.last)
        .withTargetPlatforms(platforms(platform_id))
    }

    // START iteration 1..n
    for (iteration <- 1 to m) {
      one_iteration(1, iteration) // spark
    }
    for (iteration <- m+1 to iterations-1) {
      one_iteration(0, iteration) // java
    }

    println("UnstablePoints " + AllUnstablePointsCounts
      .withTargetPlatforms(platforms(final_count_platform_id))
      .collect())
  }
}