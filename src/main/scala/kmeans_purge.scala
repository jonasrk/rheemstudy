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
    val k = 4
    val iterations = 4
    val epsilon = 0.01

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName(s"k-means ($inputUrl, k=$k, $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    case class TaggedPointCounter(x: Double, y: Double, cluster: Int, count: Long) {
      def add_points(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count)
      def average = TaggedPointCounter(x / count, y / count, cluster, 0)
    }

    // Create initial centroids.
    val random = new Random
    val initialCentroids = planBuilder
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextGaussian(), random.nextGaussian(), i, 0)).withName("Load random centroids")

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
            minDistance = distance
            closest_centroid = centroid.cluster
          }
        }


        return new TaggedPointCounter(point.x, point.y, closest_centroid, 1)


      }
    }

    val centroid_list = initialCentroids.collect()

    val points = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
      }.withName("Create points")

    val points_w_cl_centroid = points
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(initialCentroids, "centroids").withName("Find nearest centroid")

    val new_centroids = points_w_cl_centroid
      .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
      .withCardinalityEstimator(k)
      .map(_.average).withName("Average points").collect()

    var filtered_centroids = List[TaggedPointCounter]()
    var filtered_points = List[TaggedPointCounter]()

    var points_list_to_be_filtered = planBuilder // TODO JRK: This is stupid. Should not be done twice.
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
      }.withName("Create points")
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(initialCentroids, "centroids").withName("Find nearest centroid")
      .collect()

    println(new_centroids)
    println(centroid_list)

    for (new_centroid <- new_centroids){
      for (old_centroid <- centroid_list){
        if (new_centroid.cluster == old_centroid.cluster){
          val distance = Math.pow(Math.pow(old_centroid.x - new_centroid.x, 2) + Math.pow(old_centroid.y - new_centroid.y, 2), 0.5)
          println("## distance")
          println(distance)
          if (distance > epsilon) {
            filtered_centroids ::= new_centroid
            points_list_to_be_filtered = points_list_to_be_filtered.filter(_.cluster == old_centroid.cluster)
          }
        }
      }
    }

    println("## filtered_centroids")
    println(filtered_centroids)
    println("## points_list_to_be_filtered")
    println(points_list_to_be_filtered)

    val filtered_centroids_quanta_for_second_iteration = planBuilder.loadCollection(filtered_centroids)
    val filtered_points_quanta_for_second_iteration = planBuilder.loadCollection(points_list_to_be_filtered)

    val points_second_iteration = filtered_points_quanta_for_second_iteration
      .mapJava(new SelectNearestCentroidForPoint)
      .withBroadcast(filtered_centroids_quanta_for_second_iteration, "centroids").withName("Find nearest centroid")
      .collect()

    println("## points_second_iteration")
    println(points_second_iteration)

  }
}