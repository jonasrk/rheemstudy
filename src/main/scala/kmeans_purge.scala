/**
  * Created by jonas on 2/13/17.
  */
import org.qcri.rheem.api._
import org.qcri.rheem.basic.operators._
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.function.{ExecutionContext, FunctionDescriptor}
import org.qcri.rheem.core.api.Configuration
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.function._
import org.qcri.rheem.core.plan.rheemplan.RheemPlan
import org.qcri.rheem.core.types.DataSetType
import org.qcri.rheem.core.util.RheemArrays
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

import scala.util.Random
import scala.collection
import scala.collection.JavaConversions._

import java.util
import java.util.Arrays
import java.util.Collection



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








//
//
//
//    val collectorT = new util.LinkedList[Integer]
//    val numIterations = 1
//    val collector = collectorT
//    val values = 0
//    val source = new CollectionSource[Integer](RheemArrays.asList(values), classOf[Integer])
//    source.setName("source")
//
//
//    val convergenceSource = new CollectionSource[Integer](RheemArrays.asList(0), classOf[Integer])
//    convergenceSource.setName("convergenceSource")
//
//
//    val loopOperator = new LoopOperator[Integer, Integer](
//      DataSetType.createDefault(classOf[Integer]),
//      DataSetType.createDefault(classOf[Integer]),
//      (collection: Iterator[Int]) =>
//      collection.next() >= numIterations.asInstanceOf[FunctionDescriptor.SerializablePredicate[util.Collection[Integer]]],
//      numIterations)
//
//
//    loopOperator.setName("loop")
//    loopOperator.initialize(source, convergenceSource)
//    val stepOperator = new FlatMapOperator[Integer, Integer](
//    val ->
//    Arrays.asList(2 *
//    val, 2 *
//    val +
//    1000
//    ), classOf[Integer]
//    , classOf[Integer]
//    )
//    stepOperator.setName("step")
//    val counter = new MapOperator[Integer, Integer](new TransformationDescriptor[Integer, Integer](n -> n + 1, classOf[Integer], classOf[Integer]))
//    counter.setName("counter")
//    loopOperator.beginIteration(stepOperator, counter)
//    loopOperator.endIteration(stepOperator, counter)
//    val sink = LocalCallbackSink.createCollectingSink(collector, classOf[Integer])
//    sink.setName("sink")
//    loopOperator.outputConnectTo(sink)
//    val rheemPlan = new RheemPlan(sink)
//    // Instantiate Rheem and activate the Java backend.
//    val rheemContext = new RheemContext().`with`(Java.basicPlugin)
//    rheemContext.execute(rheemPlan)
//    System.out.println(collector)

  }
}