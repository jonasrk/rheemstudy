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


    case class NodeWithNeighbours(name: String, id: Int, neighbours: ArrayBuffer[String]) {
      def add_neighbour(name: String): Unit ={
        neighbours += name
      }
    }

    var NodesWithNeighbours = new ArrayBuffer[NodeWithNeighbours]()

    // for edge in list

    var id = 0;

    for (edge <- parsed_edges){

      var found_1 = false
      var found_2 = false
      for (existing_node <- NodesWithNeighbours){
        if (existing_node.name == edge._1){
          found_1 = true
          existing_node.add_neighbour(edge._2)
        }
        if (existing_node.name == edge._2){
          found_2 = true
          existing_node.add_neighbour(edge._1)
        }
      }
      if (!found_1) {
        val s = new ArrayBuffer[String]()
        s += edge._2
        val node = NodeWithNeighbours(edge._1, id, s)
        NodesWithNeighbours += node
        id += 1
      }
      if (!found_2) {
        val s = new ArrayBuffer[String]()
        s += edge._1
        val node2 = NodeWithNeighbours(edge._2, id, s)
        NodesWithNeighbours += node2
        id += 1
      }

    }

    class SelectMinimumIdOfNeighbours extends ExtendedSerializableFunction[NodeWithNeighbours, NodeWithNeighbours] {

      /** Keeps the broadcasted centroids. */
      var neighbour_nodes:  Iterable[NodeWithNeighbours] = _

      override def open(executionCtx: ExecutionContext) = {
        neighbour_nodes = executionCtx.getBroadcast[NodeWithNeighbours]("neighbour_nodes")
      }

      override def apply(node: NodeWithNeighbours): NodeWithNeighbours = {
        var minId = node.id
        for (neighbour <- node.neighbours) {
          for (neighbour_node <- neighbour_nodes){
            if (neighbour.equals(neighbour_node.name)){
              if (neighbour_node.id < node.id){
                minId = neighbour_node.id
              }
            }
          }
        }
        return new NodeWithNeighbours(node.name, minId, node.neighbours)
      }
    }


    var SelectMinimumOperators = new ListBuffer[DataQuanta[NodeWithNeighbours]]()

    // iteration ZERO

    val NodesWithNeighboursQuantum = planBuilder.loadCollection(NodesWithNeighbours)

    if (iterations > 0){
      SelectMinimumOperators += NodesWithNeighboursQuantum
        .map(x => x)
        .mapJava(new SelectMinimumIdOfNeighbours)
        .withBroadcast(NodesWithNeighboursQuantum, "neighbour_nodes")
    } else {
      SelectMinimumOperators += NodesWithNeighboursQuantum
    }

    // for i iterations:
    for (i <- 1 to iterations){

      // for every node:

      //    println(NodesWithNeighboursCollection.collect())
      SelectMinimumOperators += SelectMinimumOperators.last
        .map(x => x)
        .mapJava(new SelectMinimumIdOfNeighbours)
        .withBroadcast(SelectMinimumOperators.last, "neighbour_nodes")






      // for id in id_changed:

      // does a node with this id still exist? if no, remove from id_changed

      // if it does exist:

      // neighbour changed = false

      // for each node with this key

      // has a neighbour changed in the last iteration?

      // if no, they can be taken out and put into the result set


    }

    var results =  SelectMinimumOperators.last.collect()

    for (result <- results){
        println(result)
    }

  }
}