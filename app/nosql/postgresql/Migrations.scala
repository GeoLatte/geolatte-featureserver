package nosql.postgresql

import nosql.Utils
import play.api.Logger

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import slick.jdbc.PostgresProfile.api._

import slick.sql.SqlAction

case class Migration(version: Int, statement: SqlAction[Int, NoStream, Effect], schema: String)

object Migrations {

  import Utils._


  private def statements(schema: String) = List(

    sqlu"""
        CREATE TABLE #$schema.geolatte_nosql_migrations (version int, stmt varchar(1024) );
    """,

    sqlu"""
       alter table #$schema.geolatte_nosql_collections ADD COLUMN  geometry_col VARCHAR(255);
       alter table #$schema.geolatte_nosql_collections ADD COLUMN  pkey VARCHAR(255);
     """)

  def list(schema: String) = for {
    (stmt, index) <- statements(schema).zipWithIndex
  } yield Migration(index, stmt, schema)

  def executeOn(schema: String)(implicit db: Database): Future[Boolean] =
    for {
      n <- getLatestMigrationInSchema(schema)
      migrations = migrationsToApply(schema, n)
      res <- applyMigrations(migrations)
    } yield res

  private def migrationsToApply(schema: String, n: Int) =
    list(schema).filter(_.version > n)

  private def getLatestMigrationInSchema(schema: String)(implicit db: Database): Future[Int] = {
    val dbio = sql"select max(version) from #$schema.geolatte_nosql_migrations".as[Int].headOption
    db.run(dbio).map {
      case Some(i) => i
      case _ => 0
    }.recover {
      case e =>
        withWarning(s"Error on getting latest migration for schema $schema. Error ${e.getMessage}")(-1)
    }
  }

  private def applyMigration(migration: Migration)(implicit db: Database): Future[Boolean] = {
    Logger.info(s"Starting migration ${migration.version} on ${migration.schema}")
    import scala.language.existentials //TODO -- remove reliance on this feature
    val stmt = migration.statement andThen registerMigrationStmt(migration)
    db.run(stmt.transactionally)
      .map(_ => withInfo(s"Migration ${migration.version} successfully applied")(true))
      .recover {
        case t => withError(s"Migration ${migration.version} failed on schema ${migration.schema}\n ${stmt}")(false)
      }
  }

  //TODO -- replace sequence here with DBIO combinators
  private def applyMigrations(migrations: List[Migration])(implicit db: Database): Future[Boolean] =
    sequence(migrations)(applyMigration _)


  private def registerMigrationStmt(migration: Migration) = {
    def toString(action: SqlAction[_, _, _]): String = action.statements.toList.mkString(";")
    sqlu"""
       insert into #${migration.schema}.geolatte_nosql_migrations values(${migration.version}, '${toString(migration.statement)}');
     """
  }
}

