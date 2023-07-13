package akka_akka_streams.homework

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{RunnableGraph, ZipWith}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip}

object homework {
  implicit val system = ActorSystem("fusion")
  implicit val materializer = ActorMaterializer()
  val graph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val input =  builder.add(Source(1 to 5))

    val broadcast = builder.add(Broadcast[Int](3))

    val flow_1 = builder.add(Flow[Int].map(x => x * 10))
    val flow_2 = builder.add(Flow[Int].map(x => x * 2))
    val flow_3 = builder.add(Flow[Int].map(x => x * 3))

    val zip_1, zip_2 = builder.add(ZipWith[Int, Int, Int]((left, right) => {
      left + right
    }))

    val output = builder.add(Sink.foreach(println))

    input ~> broadcast

    broadcast.out(0) ~> flow_1 ~> zip_1.in0
    broadcast.out(1) ~> flow_2 ~> zip_1.in1

    zip_1.out ~> zip_2.in0
    broadcast.out(2) ~> flow_3 ~> zip_2.in1

    zip_2.out ~> output

    ClosedShape
  }
  def main(args: Array[String]) : Unit ={
    RunnableGraph.fromGraph(graph).run()

  }
}