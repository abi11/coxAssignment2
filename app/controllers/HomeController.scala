package controllers

import javax.inject._

import actors._
import akka.NotUsed
import akka.actor._
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.util.Timeout
import play.api.Logger
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


@Singleton
class HomeController @Inject()(@Named("userParentActor") userParentActor: ActorRef,
                               cc: ControllerComponents)
                              (implicit ec: ExecutionContext)
  extends AbstractController(cc) with SameOriginCheck {

  val logger = play.api.Logger(getClass)

  
  def index = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index())
  }

 
  def ws: WebSocket = WebSocket.acceptOrResult[JsValue, JsValue] {
    case rh if sameOriginCheck(rh) =>
      wsFutureFlow(rh).map { flow =>
        Right(flow)
      }.recover {
        case e: Exception =>
          logger.error("Cannot create websocket", e)
          val jsError = Json.obj("error" -> "Cannot create websocket")
          val result = InternalServerError(jsError)
          Left(result)
      }

    case rejected =>
      logger.error(s"Request ${rejected} failed same origin check")
      Future.successful {
        Left(Forbidden("forbidden"))
      }
  }


  private def wsFutureFlow(request: RequestHeader): Future[Flow[JsValue, JsValue, NotUsed]] = {
    implicit val timeout = Timeout(10.second) 
    val future: Future[Any] = userParentActor ? UserParentActor.Create(request.id.toString)
    val futureFlow: Future[Flow[JsValue, JsValue, NotUsed]] = future.mapTo[Flow[JsValue, JsValue, NotUsed]]
    futureFlow
  }

}

trait SameOriginCheck {

  def logger: Logger

  
  def sameOriginCheck(rh: RequestHeader): Boolean = {
    rh.headers.get("Origin") match {
      case Some(originValue) if originMatches(originValue) =>
        logger.debug(s"originCheck: originValue = $originValue")
        true

      case Some(badOrigin) =>
        logger.error(s"originCheck: rejecting request because Origin header value ${badOrigin} is not in the same origin")
        false

      case None =>
        logger.error("originCheck: rejecting request because no Origin header found")
        false
    }
  }

  
  def originMatches(origin: String): Boolean = {
    origin.contains("localhost:9000") || origin.contains("localhost:19001")
  }

}