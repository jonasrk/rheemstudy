/**
  * Created by jonas on 2/13/17.
  */
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.function.{ExecutionContext, FunctionDescriptor}
import org.qcri.rheem.core.util.RheemCollections
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

import scala.util.Random
import scala.collection.JavaConversions._

import java.util

object kmeans_purge {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/Users/jonas/tmp_kmeans_big.txt"
    val k = 10
    val iterations = 3
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
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextFloat(), random.nextFloat(), i, 0)).withName("Load random centroids")

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

    class DoubleX extends ExtendedSerializableFunction[TaggedPointCounter, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(point: TaggedPointCounter): TaggedPointCounter = {
        return new TaggedPointCounter(point.x * 2, point.y, 0, 1)
      }
    }



    // Generate some test data.
    val inputValues = Array(1, 2, 3, 4)
    // Generate some test data.



    val output = planBuilder.loadCollection(inputValues)
      .doWhile[Int](condition => condition.size > 2, {pointsx =>
      val res = pointsx.map(numb => numb + 23)
      val condition_out = pointsx.map(numb => 2000 + numb)
      (res.sample(res.count - 1), condition_out)
    })


    println(output.collect())




    //    val initial_centroids_list = initialCentroixds.collect()
    //
    //    val initial_centroids_map = initialCentroids
    //      .map{ point => point}
    //
    //    println("initialCentroids before:")
    //    println(initialCentroids)
    //    println("initial_centroids_list before:")
    //    println(initial_centroids_list)
    //
    //    val condition = planBuilder.loadCollection(Iterable(1, 2, 3))
    //
    //    val output = initial_centroids_map
    //      .doWhile[Int](condition => condition.max > 100, {pointsx =>
    //      val res = pointsx
    //        .map(point => TaggedPointCounter(point.x * 2, point.y, point.cluster, 0))
    //      val condition_out = condition.map(numb => 2 * numb)
    //      (condition_out, condition_out)
    //
    //      })
    //
    ////    val output = initial_centroids_map
    ////      .repeat(iterations, {pointsx => pointsx
    ////            .map(point => TaggedPointCounter(point.x * 2, point.y, point.cluster, 0))
    ////      }).collect()
    //
    //    println("output after:")
    //    println(output.collect())



    //    val initial_centroid_list = initialCentroids.collect()
    //
    //    val initial_points_list = planBuilder
    //      .readTextFile(inputUrl).withName("Read file")
    //      .map { line =>
    //        val fields = line.split(",")
    //        TaggedPointCounter(fields(0).toDouble, fields(1).toDouble, 0, 1)
    //      }.withName("Create points").collect()
    //
    //    var filtered_centroids = List[TaggedPointCounter]()
    //    var filtered_points = Iterable[TaggedPointCounter]()
    //
    //
    //    println("## initial_centroid_list: ")
    //    println(initial_centroid_list)
    //
    //    println("## initial_points_list: ")
    //    println(initial_points_list)
    //
    //
    //    var i = 0
    //    while (i < iterations) {
    //
    //      var iteration_start_centroids = Iterable[TaggedPointCounter]()
    //      var iteration_start_points = Iterable[TaggedPointCounter]()
    //
    //      if (i == 0) {
    //        iteration_start_centroids = initial_centroid_list
    //        iteration_start_points = initial_points_list
    //      } else {
    //        iteration_start_centroids = filtered_centroids
    //        iteration_start_points = filtered_points
    //      }
    //
    //      val iteration_start_centroids_quanta = planBuilder.loadCollection(iteration_start_centroids)
    //
    //      var points_w_cl_centroid_list = planBuilder.loadCollection(iteration_start_points)
    //        .mapJava(new SelectNearestCentroidForPoint)
    //        .withBroadcast(iteration_start_centroids_quanta, "centroids").withName("Find nearest centroid")
    //        .collect()
    //
    //      val new_centroids_list = planBuilder.loadCollection(points_w_cl_centroid_list)
    //        .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
    //        .withCardinalityEstimator(k)
    //        .map(_.average).withName("Average points").collect()
    //
    //      filtered_centroids = List[TaggedPointCounter]()
    //      filtered_points = points_w_cl_centroid_list
    //
    //      for (new_centroid <- new_centroids_list) {
    //        for (old_centroid <- iteration_start_centroids) {
    //          if (new_centroid.cluster == old_centroid.cluster) {
    //            val distance = Math.pow(Math.pow(old_centroid.x - new_centroid.x, 2) + Math.pow(old_centroid.y - new_centroid.y, 2), 0.5)
    //            if (distance < epsilon) {
    //              filtered_points = filtered_points.filter(_.cluster != new_centroid.cluster)
    //            } else {
    //              filtered_centroids ::= new_centroid
    //            }
    //          }
    //        }
    //      }
    //
    //      println("## After iteration " + i)
    //      println("filtered_centroids:")
    //      println(filtered_centroids)
    //      println("filtered_points:")
    //      println(filtered_points)
    //
    //      i += 1
    //    }
  }
}