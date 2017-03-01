/**
  * Created by jonas on 2/13/17.
  */
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark
import scala.util.Random
import scala.collection.JavaConversions._

object kmeans_purge {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/Users/jonas/tmp_kmeans.txt"
    val k = 2
    val iterations = 4

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName(s"k-means ($inputUrl, k=$k, $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    case class Point(x: Double, y: Double)
    case class TaggedPoint(x: Double, y: Double, old_x: Double, old_y: Double, cluster: Int)
    case class TaggedPointCounter(x: Double, y: Double, cluster: Int, count: Long) {
      def add_points(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count)
      def average = TaggedPointCounter(x / count, y / count, cluster, 0)
    }

    // Read and parse the input file(s).
    val points = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        Point(fields(0).toDouble, fields(1).toDouble)
      }.withName("Create points")


    // Create initial centroids.
    val random = new Random
    val initialCentroids = planBuilder
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextGaussian(), random.nextGaussian(), i, 0)).withName("Load random centroids")

    // Declare UDF to select centroid for each data point.
    class SelectNearestCentroid extends ExtendedSerializableFunction[Point, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(point: Point): TaggedPointCounter = {
        var minDistance = Double.PositiveInfinity
        var nearestCentroidId = -1
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance) {
            minDistance = distance
            nearestCentroidId = centroid.cluster
          }
        }
        if (nearestCentroidId != -1){
          println("### Not with a removed centroid")
          return new TaggedPointCounter(point.x, point.y, -1, 1)
        } else {
          println("### With a removed centroid")
          return new TaggedPointCounter(point.x, point.y, -1, 1)
        }
      }
    }

    // Declare UDF to eliminate centroids that did not change much.
    class EliminateCentroids extends ExtendedSerializableFunction[TaggedPointCounter, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(newCentroid: TaggedPointCounter): TaggedPointCounter = {
        var change_too_small = false
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(newCentroid.x - centroid.x, 2) + Math.pow(newCentroid.y - centroid.y, 2), 0.5)
          if (distance < 0.00001) {
            change_too_small = true
          }
        }
        if (change_too_small == true) {
          println("### too small!")
          return new TaggedPointCounter(newCentroid.x, newCentroid.y, -1, 1)
        } else {
          println("### NOT too small!")
          return new TaggedPointCounter(newCentroid.x, newCentroid.y, newCentroid.cluster, 0)
        }

      }
    }

    // Do the k-means loop.
    //    val finalCentroids = initialCentroids.repeat(iterations, { currentCentroids =>
    //      points
    //        .mapJava(new SelectNearestCentroid)
    //        .withBroadcast(currentCentroids, "centroids").withName("Find nearest centroid")
    //        .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
    //        .withCardinalityEstimator(k)
    //        .map(_.average).withName("Average points")
    //        .mapJava(new EliminateCentroids)
    //        .withBroadcast(currentCentroids, "centroids").withName("Find nearest centroid")
    //    }).withName("Loop")
    //
    //      // Collect the results.
    //      .collect()
    //
    //    println(finalCentroids)


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
        for (centroid <- centroids){
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance){
            closest_centroid = centroid.cluster
          }
        }


        return new TaggedPointCounter(point.x, point.y, closest_centroid, 1)


      }
    }


    val centroids = initialCentroids.collect()
    println("centroids:")
    println(centroids)

    val points2 = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
      }.withName("Create points")

    //    val points2_output = points2.collect()
    //
    //    println("points2_output:")
    //    println(points2_output)

    val points3 = points2
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(initialCentroids, "centroids").withName("Find nearest centroid")

    val newCentroids = points3
      .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
      .withCardinalityEstimator(k)
      .map(_.average).withName("Average points").collect()

    // compare new to old centroids

    println(newCentroids)
    println(centroids)

    var filtered_centroids = List[TaggedPointCounter]()
    var filtered_points = List[TaggedPointCounter]()

    var points_list = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
      }.withName("Create points")
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(initialCentroids, "centroids").withName("Find nearest centroid")
      .collect()

    for (new_centroid <- newCentroids){
      for (old_centroid <- centroids){
        if (new_centroid.cluster == old_centroid.cluster){
          val distance = Math.pow(Math.pow(old_centroid.x - new_centroid.x, 2) + Math.pow(old_centroid.y - new_centroid.y, 2), 0.5)
          println(distance)
          if (distance > 0.1) {
            filtered_centroids ::= new_centroid
            points_list = points_list.filter(_.cluster == old_centroid.cluster)
          }
        }
      }
    }

    println("filtered_centroids")
    println(filtered_centroids)
    println("points_list")
    println(points_list)

    val filtered_centroids_quanta = planBuilder.loadCollection(filtered_centroids)

    println(filtered_centroids_quanta)

    val pointsagain = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
      }.withName("Create points")

    val points_second_iteration = pointsagain
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(filtered_centroids_quanta, "centroids").withName("Find nearest centroid")
      .collect()

    println(points_second_iteration)

  }
}