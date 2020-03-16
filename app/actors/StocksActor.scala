package actors

import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingReceive
import stocks._

import scala.collection.mutable


class StocksActor extends Actor with ActorLogging {
  import Messages._

  // remove stocks 
  private val stocksMap: mutable.Map[StockSymbol, Stock] = mutable.HashMap()

  def receive = LoggingReceive {
    case WatchStocks(symbols) =>
      val stocks = symbols.map(symbol => stocksMap.getOrElseUpdate(symbol, new Stock(symbol)))
      sender ! Stocks(stocks)
  }
}
