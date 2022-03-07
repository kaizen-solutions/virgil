//package io.kaizensolutions.virgil
//
//import com.datastax.oss.driver.api.core.data.GettableByName
//import io.kaizensolutions.virgil.codecs.{CqlColumnDecoder, CqlDecoder, UdtDecoder}
//import zio.Chunk
//
//import scala.util.control.NonFatal
//
//sealed trait Cursor {
//  def downUdt(name: String): Either[String, Cursor]
//  def field[A](name: String)(implicit ev: CqlColumnDecoder[A]): Either[String, A]
//  def viewCursorAs[A](implicit ev: CqlColumnDecoder[A]): Either[String, A]
//}
//object Cursor {
//  def row[A](cursor: Cursor => Either[String, A]): CqlDecoder[Either[String, A]] =
//    CqlColumnDecoder
//      .fromRow(row => cursor(new GettableByNameCursor(Chunk.empty, row)))
//
//  def udt[A](cursor: Cursor => Either[String, A]): UdtDecoder[Either[String, A]] =
//    CqlColumnDecoder.fromUdtValue(row => cursor(new GettableByNameCursor(Chunk.empty, row)))
//}
//
//private class GettableByNameCursor(history: Chunk[String], current: GettableByName) extends Cursor {
//  override def downUdt(name: String): Either[String, Cursor] =
//    try Right(new GettableByNameCursor(history :+ "name", current.getUdtValue(name)))
//    catch {
//      case NonFatal(e) => Left(renderError(s"focusing down on $name", e))
//    }
//
//  override def field[A](name: String)(implicit ev: CqlColumnDecoder[A]): Either[String, A] =
//    try Right(ev.decodeFieldByName(current, name))
//    catch {
//      case NonFatal(e) => Left(renderError(s"reading $name", e))
//    }
//
//  private def renderError(action: String, error: Throwable): String = {
//    val renderedHistory = history.mkString(start = "History: ", sep = " -> ", end = " ")
//    val message         = s"Error $action: ${error.getMessage}"
//    renderedHistory + message
//  }
//
//  override def viewCursorAs[A](implicit ev: CqlColumnDecoder[A]): Either[String, A] =
//    try Right(ev.decodeFieldByName(current, null))
//    catch {
//      case NonFatal(e) => Left(renderError("viewing current level", e))
//    }
//}
