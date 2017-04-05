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
import scala.collection.mutable.ListBuffer
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

    println(edges.collect())


    // give every node an id



    // for i iterations:

      // for every node:

        // mininum = max id

        // for each neighbour:

          // if neighbur id < minimum:

            // minumum = neighbour_id

        // if node_id < minumum:

          // node_id = minimum

      // for id in id_changed:

        // does a node with this id still exist? if no, remove from id_changed

        // if it does exist:

          // neighbour changed = false

          // for each node with this key

            // has a neighbour changed in the last iteration?

              // if no, they can be taken out and put into the result set



  }
}