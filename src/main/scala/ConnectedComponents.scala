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
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ConnectedComponents {
  def main(args: Array[String]): Unit = {

    // Parse command line parameters
    var inputUrlNodes = ""
    var iterations = -1

    val platforms = Array(Java.platform, Spark.platform)
    var first_iteration_platform_id, final_count_platform_id, m = 0

    iterations = args(1).toInt
    inputUrlNodes = args(2)

    val platform = args(0)
    if (platform.equals("mixed")){
      first_iteration_platform_id = 1
      final_count_platform_id = 0
      m = args(3).toInt
    } else if (platform.equals("spark")) {
      first_iteration_platform_id = 1
      final_count_platform_id = 1
      m = iterations - 1
    } else if (platform.equals("java")) {
      first_iteration_platform_id = 0
      final_count_platform_id = 0
      m = 0
    }

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName(s"connected_components ($inputUrlNodes, $platform, m=$m, $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    // read .nt file
    def parseTriple(raw: String): (String, String, String) = {
      // Find the first two spaces: Odds are that these are separate subject, predicated and object.
      val firstSpacePos = raw.indexOf(' ')
      val secondSpacePos = raw.indexOf(' ', firstSpacePos + 1)

      // Find the end position.
      var stopPos = raw.lastIndexOf('.')
      while (raw.charAt(stopPos - 1) == ' ') stopPos -= 1

      (raw.substring(0, firstSpacePos),
        raw.substring(firstSpacePos + 1, secondSpacePos),
        raw.substring(secondSpacePos + 1, stopPos))
    }

    // Read and parse the input file.
    val edges = planBuilder
      .readTextFile(inputUrlNodes).withName("Load file")
      .filter(!_.startsWith("#"), selectivity = 1.0).withName("Filter comments")
      .map(parseTriple).withName("Parse triples")
      .map { case (s, p, o) => (s, o) }.withName("Discard predicate")

    val parsed_edges = edges.collect()

    case class edge(unique_edge_id: Int, src: Int, minId: Int, target: Int, has_changed: Int) {
      def min_id(that: edge) = edge(this.unique_edge_id, this.src, scala.math.min(this.minId, that.minId), this.target, this.has_changed)
    }

    var NodesWithNeighbours = new ArrayBuffer[edge]()

    // creating a map
    var id_mapping = scala.collection.mutable.Map[String, Int]()
    var mapping_id = 0
    for (edge <- parsed_edges){
      if (!(id_mapping contains edge._1)){
        id_mapping += (edge._1 -> mapping_id)
        mapping_id += 1
      }
      if (!(id_mapping contains edge._2)){
        id_mapping += (edge._2 -> mapping_id)
        mapping_id += 1
      }
    }

    // creating a graph of ids
    var unique_edge_id = 0
    for (parsed_edge <- parsed_edges){
      val node = edge(unique_edge_id, id_mapping(parsed_edge._1), id_mapping(parsed_edge._1), id_mapping(parsed_edge._2), 1)
      NodesWithNeighbours += node
      unique_edge_id += 1
      val node2 = edge(unique_edge_id, id_mapping(parsed_edge._2), id_mapping(parsed_edge._2), id_mapping(parsed_edge._1), 1)
      NodesWithNeighbours += node2
      unique_edge_id += 1
    }

    class TagStableEdges extends ExtendedSerializableFunction[edge, edge] {
      /** Keeps the broadcasted centroids. */
      var unstable_ids: Iterable[Tuple2[Int, Int]] = _

      override def open(executionCtx: ExecutionContext) = {
        unstable_ids = executionCtx.getBroadcast[Tuple2[Int, Int]]("unstable_ids")
      }

      override def apply(edge: edge): edge = {
        var found = false
        for (unstable_id <- unstable_ids) {
          if (unstable_id._1 == edge.minId){
            found = true
          }
        }
        if (found == true){
          return new edge(edge.unique_edge_id, edge.src, edge.minId, edge.target, -1)
        } else {
          return new edge(edge.unique_edge_id, edge.src, edge.minId, edge.target, 1)
        }
      }
    }


    // START iteration ZERO

    val NodesWithNeighboursQuantum = planBuilder.loadCollection(NodesWithNeighbours)

    var SelectMinimumAndReduceOperator = new ListBuffer[DataQuanta[edge]]
    var JoinOperator = new ListBuffer[DataQuanta[org.qcri.rheem.basic.data.Tuple2[edge, edge]]]

    if (iterations > 0){

      JoinOperator += NodesWithNeighboursQuantum
        .map(x => x)
        .join(_.src, NodesWithNeighboursQuantum, _.target)

      SelectMinimumAndReduceOperator += JoinOperator.last
        .map(x => {
          if (x.field0.minId == x.field1.minId){
            edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 0)
          } else {
            edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 1)
          }
        })
        .reduceByKey(_.unique_edge_id, _.min_id(_))
    } else {
      SelectMinimumAndReduceOperator += NodesWithNeighboursQuantum
    }

    var IdUpdate, filter_stable, filter_unstable = new ListBuffer[DataQuanta[Tuple2[Int, Int]]]
    var UnstableEdges = new ListBuffer[DataQuanta[edge]]

    UnstableEdges += SelectMinimumAndReduceOperator.last
      .filter(_.has_changed != -1) // TODO JRK This is not actually doing something, just initializing the var, probably stupid

    var StableEdges = SelectMinimumAndReduceOperator.last
      .filter(_.has_changed == -1) // TODO JRK This is not actually doing something, just initializing the var, probably stupid

    // for i iterations:
    for (i <- 1 to iterations - 1){

      IdUpdate += SelectMinimumAndReduceOperator.last
        .map(x => (x.minId, x.has_changed))
        .reduceByKey(_._1, (x, y) => (x._1, scala.math.max(x._2, y._2)))

      filter_stable += IdUpdate.last
        .filter(_._2 == 0)

      filter_unstable += IdUpdate.last
        .filter(_._2 == 0)

      UnstableEdges += SelectMinimumAndReduceOperator.last
        .mapJava(new TagStableEdges)
        .withBroadcast(filter_unstable.last, "unstable_ids")
        .filter(_.has_changed != -1)

      StableEdges = StableEdges
        .union(
          SelectMinimumAndReduceOperator.last
            .mapJava(new TagStableEdges)
            .withBroadcast(filter_unstable.last, "unstable_ids")
            .filter(_.has_changed == -1)
        )

      JoinOperator += UnstableEdges.last
        .map(x => x)
        .join(_.src, SelectMinimumAndReduceOperator.last, _.target)

      SelectMinimumAndReduceOperator += JoinOperator.last
        .map(x => {
          if (x.field0.minId == x.field1.minId){
            edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 0)
          } else {
            edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 1)
          }
        })
        .reduceByKey(_.unique_edge_id, _.min_id(_))
    }

//    var results = StableEdges.count.collect()
var results = UnstableEdges.last.count.collect()
    println(results)
//    for (result <- results){
//      println(result)
//    }
  }
}