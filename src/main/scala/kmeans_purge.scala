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
    val iterations = 2
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
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance) {
            minDistance = distance
            closest_centroid = centroid.cluster
          }
        }


        return new TaggedPointCounter(point.x, point.y, closest_centroid, 1)


      }
    }



    val initial_centroid_list = initialCentroids.collect()

    val initial_points_list = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
      }.withName("Create points").collect()


    var filtered_centroids = List[TaggedPointCounter]()
    var filtered_points = List[TaggedPointCounter]()

    var i = 0
    while (i < iterations) {
      i += 1

      var iteration_start_centroids = Iterable[TaggedPointCounter]()
      var iteration_start_points = Iterable[TaggedPointCounter]()

      if (i == 0) {
        iteration_start_centroids = initial_centroid_list
        iteration_start_points = initial_points_list
      } else {
        iteration_start_centroids = filtered_centroids
        iteration_start_points = filtered_points

      }

      val iteration_start_centroids_quanta = planBuilder.loadCollection(iteration_start_centroids)

      var points_w_cl_centroid_list = planBuilder.loadCollection(iteration_start_points)
        .mapJava(new SelectNearestCentroidForPoint)
        .withBroadcast(iteration_start_centroids_quanta, "centroids").withName("Find nearest centroid")
        .collect()

      val new_centroids_list = planBuilder.loadCollection(points_w_cl_centroid_list)
        .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
        .withCardinalityEstimator(k)
        .map(_.average).withName("Average points").collect()

      filtered_centroids = List[TaggedPointCounter]()
      filtered_points = List[TaggedPointCounter]()

      for (new_centroid <- new_centroids_list) {
        for (old_centroid <- iteration_start_centroids) {
          if (new_centroid.cluster == old_centroid.cluster) {
            val distance = Math.pow(Math.pow(old_centroid.x - new_centroid.x, 2) + Math.pow(old_centroid.y - new_centroid.y, 2), 0.5)
            println("## distance")
            println(distance)
            if (distance > epsilon) {
              filtered_centroids ::= new_centroid
              points_w_cl_centroid_list = points_w_cl_centroid_list.filter(_.cluster == old_centroid.cluster)
            }
          }
        }
      }
    }
  }
}