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

object ConnectedComponents {
  def main(args: Array[String]): Unit = {

    // read .nt file

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