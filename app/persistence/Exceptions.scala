/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/10/13
 */
package object Exceptions {

  abstract class NoSqlException(msg: String) extends RuntimeException(msg)

  case class DatabaseNotFoundException(msg: String = "") extends NoSqlException(msg)

  case class CollectionNotFoundException(msg: String = "") extends NoSqlException(msg)

  case class IndexNotFoundException(msg: String = "") extends NoSqlException(msg)

  case class NoSpatialMetadataException(msg: String = "") extends NoSqlException(msg)

  case class DatabaseAlreadyExistsException(msg: String = "") extends NoSqlException(msg)

  case class CollectionAlreadyExistsException(msg: String = "") extends NoSqlException(msg)

  case class DatabaseCreationException(msg: String = "") extends NoSqlException(msg)

  case class DatabaseDeleteException(msg: String = "") extends NoSqlException(msg)

  case class InvalidQueryException(msg: String = "") extends NoSqlException(msg)

  case class UnsupportedMediaException(msg: String = "") extends NoSqlException(msg)

  case class ViewObjectNotFoundException(msg: String = "") extends NoSqlException(msg)

  case class InvalidParamsException(msg: String = "") extends NoSqlException(msg)

  case class NotReadyException(msg: String) extends NoSqlException(msg)

  case class InvalidPrimaryKeyException(msg: String) extends NoSqlException(msg)

  case class InvalidRequestException(msg: String) extends NoSqlException(msg)

}
