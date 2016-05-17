package nosql.postgresql

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.pool.ConnectionPool
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import nosql.Utils
import play.api.Logger

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

case class Migration(version: Int, statement: String, schema: String)

object Migrations {

  import Utils._
  import PostgresqlRepository.Sql

  private def statements(schema: String) = List(

    s"""
       | CREATE TABLE $schema.geolatte_nosql_migrations (version int, stmt varchar(1024) );
    """.stripMargin,

    s"""
       |alter table $schema.geolatte_nosql_collections ADD COLUMN  geometry_col VARCHAR(255);
       |alter table $schema.geolatte_nosql_collections ADD COLUMN  pkey VARCHAR(255);
     """.stripMargin

  )

  def list(schema: String) = for {
    (stmt, index) <- statements(schema).zipWithIndex
  } yield Migration(index, stmt, schema)


  def executeOn(schema: String)(implicit cp: ConnectionPool[PostgreSQLConnection]): Future[Boolean] =
    for {
      n <- getLatestMigrationInSchema(schema)
      migrations =  migrationsToApply(schema, n)
      res <- applyMigrations(migrations)
    } yield res

  private def migrationsToApply(schema: String, n: Int) =
    list(schema).filter(_.version > n)

  private def getLatestMigrationInSchema(schema: String)(implicit c: Connection): Future[Int] =
    c.sendQuery(s"select max(version) from $schema.geolatte_nosql_migrations").map {
      _.rows.map(rs => rs(0)(0).asInstanceOf[Int]).getOrElse(-1)
    } recover {
      case e =>
        withWarning(s"Error on getting latest migration for schema $schema. Error ${e.getMessage}")(-1)
    }

  private def applyMigration(migration: Migration)(implicit c: Connection): Future[Boolean] = {
    Logger.info(s"Starting migration ${migration.version} on ${migration.schema}")
    val stmt = migration.statement + "\n" +registerMigrationStmt(migration)
    c.inTransaction(implicit c =>
      c.sendQuery(stmt)
    ).map(_ => withInfo(s"Migration ${migration.version} successfully applied")(true)
    ).recover {
      case t => withError(s"Migration ${migration.version} failed on schema ${migration.schema}\n ${stmt}")(false)
    }
  }

  private def applyMigrations(migrations: List[Migration])(implicit c:  Connection): Future[Boolean] =
    sequence(migrations)(applyMigration _)
//    migrations.foldLeft( Future.successful(true) ) { (res, m) => res.flatMap( ok => applyMigration(m).map(_ && ok)) }

  private def registerMigrationStmt(migration: Migration) =
    s"""
       | insert into ${migration.schema}.geolatte_nosql_migrations values(${migration.version}, '${migration.statement.trim}');
       |
     """.stripMargin



}


