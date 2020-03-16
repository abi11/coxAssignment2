package actors

import javax.inject._

import akka.actor._
import akka.event.{LogMarker, MarkerLoggingAdapter}
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.google.inject.assistedinject.Assisted
import play.api.libs.json._
import stocks._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


class UserActor @Inject()(@Assisted id: String, @Named("stocksActor") stocksActor: ActorRef)
                         (implicit mat: Materializer, ec: ExecutionContext)
  extends Actor {
  import Messages._

  
  private val marker = LogMarker(name = self.path.name)
  implicit val log: MarkerLoggingAdapter = akka.event.Logging.withMarker(context.system, this.getClass)

  implicit val timeout = Timeout(50.millis)

  val (hubSink, hubSource) = MergeHub.source[JsValue](perProducerBufferSize = 16)
    .toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both)
    .run()

  private var stocksMap: Map[StockSymbol, UniqueKillSwitch] = Map.empty

  private val jsonSink: Sink[JsValue, Future[Done]] = Sink.foreach { json =>
    
    val symbol = (json \ "symbol").as[StockSymbol]
    addStocks(Set(symbol))
  }


  override def postStop(): Unit = {
    log.info(marker, s"Stopping actor $self")
    unwatchStocks(stocksMap.keySet)
  }


  override def receive: Receive = {
    case WatchStocks(symbols) =>
      addStocks(symbols)
      sender() ! websocketFlow

    case UnwatchStocks(symbols) =>
      unwatchStocks(symbols)
  }


  private lazy val websocketFlow: Flow[JsValue, JsValue, NotUsed] = {

    Flow.fromSinkAndSourceCoupled(jsonSink, hubSource).watchTermination() { (_, termination) =>
     
      termination.foreach(_ => context.stop(self))
      NotUsed
    }
  }


  private def addStocks(symbols: Set[StockSymbol]): Future[Unit] = {
    import akka.pattern.ask


    val future = (stocksActor ? WatchStocks(symbols)).mapTo[Stocks]

    
    future.map { (newStocks: Stocks) =>
      newStocks.stocks.foreach { stock =>
        if (! stocksMap.contains(stock.symbol)) {
          log.info(marker, s"Adding stock $stock")
          addStock(stock)
        }
      }
    }
  }


  private def addStock(stock: Stock): Unit = {
    
    val historySource = stock.history(5).map(sh => Json.toJson(sh))
    val updateSource = stock.update.map(su => Json.toJson(su))
    val stockSource = historySource.concat(updateSource)

    
    val killswitchFlow: Flow[JsValue, JsValue, UniqueKillSwitch] = {
      Flow.apply[JsValue]
        .joinMat(KillSwitches.singleBidi[JsValue, JsValue])(Keep.right)
        .backpressureTimeout(1.seconds)
    }

   
    val graph: RunnableGraph[UniqueKillSwitch] = {
      stockSource
        .viaMat(killswitchFlow)(Keep.right)
        .to(hubSink)
        .named(s"stock-${stock.symbol}-$id")
    }


    val killSwitch = graph.run()


    stocksMap += (stock.symbol -> killSwitch)
  }

  def unwatchStocks(symbols: Set[StockSymbol]): Unit = {
    symbols.foreach { symbol =>
      stocksMap.get(symbol).foreach { killSwitch =>
        killSwitch.shutdown()
      }
      stocksMap -= symbol
    }
  }
}


object UserActor {
  trait Factory {
    def apply(id: String): Actor
  }
}
