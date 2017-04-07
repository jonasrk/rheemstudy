/**
  * Created by jonas on 2/13/17.
  */
import org.apache.spark.graphx.lib.PageRank
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object ConnectedComponents {
  def main(args: Array[String]): Unit = {

    // Settings
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
      .withJobName(s"k-means ($inputUrlNodes, $platform, m=$m, $iterations iterations)")
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

    // new list of node objects

    //Node with Neighbours
    case class edge(unique_edge_id: Int, src: Int, minId: Int, target: Int, has_changed: Int) {
      def min_id(that: edge) = edge(this.unique_edge_id, this.src, scala.math.min(this.minId, that.minId), this.target, this.has_changed)
    }

    var NodesWithNeighbours = new ArrayBuffer[edge]()

    // for edge in list

    var id_changed = new ArrayBuffer[Int]()
    var id_mapping = scala.collection.mutable.Map[String, Int]()
    var mapping_id = 0

    // creating a map
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

    var unique_edge_id = 0
    // creating a graph of ids
    for (parsed_edge <- parsed_edges){
      val node = edge(unique_edge_id, id_mapping(parsed_edge._1), id_mapping(parsed_edge._1), id_mapping(parsed_edge._2), 1)
      NodesWithNeighbours += node
      unique_edge_id += 1

      val node2 = edge(unique_edge_id, id_mapping(parsed_edge._2), id_mapping(parsed_edge._2), id_mapping(parsed_edge._1), 1)
      NodesWithNeighbours += node2
      unique_edge_id += 1
    }



    var SelectMinimumOperators = new ListBuffer[DataQuanta[edge]]
    var SelectMinimumOperators2 = new ListBuffer[DataQuanta[org.qcri.rheem.basic.data.Tuple2[edge, edge]]]


    // iteration ZERO

    val NodesWithNeighboursQuantum = planBuilder.loadCollection(NodesWithNeighbours)

    if (iterations > 0){

      SelectMinimumOperators2 += NodesWithNeighboursQuantum
        .map(x => x)
        .join(_.src, NodesWithNeighboursQuantum, _.target)

      SelectMinimumOperators += SelectMinimumOperators2.last
        .map(x => {
          if (x.field0.minId == x.field1.minId){
            new edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 0)
          } else {
            new edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 1)
          }
        })
        .reduceByKey(_.unique_edge_id, _.min_id(_))

      SelectMinimumOperators2 += SelectMinimumOperators.last
        .map(x => x)
        .join(_.src, SelectMinimumOperators.last, _.target)

      SelectMinimumOperators += SelectMinimumOperators2.last
        .map(x => {
          if (x.field0.minId == x.field1.minId){
            new edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 0)
          } else {
            new edge(x.field0.unique_edge_id, x.field0.src, scala.math.min(x.field0.minId, x.field1.minId), x.field0.target, 1)
          }
        })
        .reduceByKey(_.unique_edge_id, _.min_id(_))

      var IdUpdate = SelectMinimumOperators.last
        .map(x => (x.minId, x.has_changed))
        .reduceByKey(_._1, (x, y) => (x._1, scala.math.max(x._2, y._2)))


      var results = IdUpdate.collect()
      for (result <- results){
        println(result)
      }

      //        .map(x => x)
      //        .mapJava(new SelectMinimumIdOfNeighbours)
      //        .withBroadcast(NodesWithNeighboursQuantum, "neighbour_nodes")




    } else {
      //      SelectMinimumOperators += NodesWithNeighboursQuantum
    }

    // for i iterations:
    for (i <- 1 to iterations){


    }


  }



}