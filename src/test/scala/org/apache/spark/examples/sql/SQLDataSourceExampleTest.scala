package org.apache.spark.examples.sql

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.{SharedSparkSession, SharedSparkSessionBase}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, ConfigMap, Filter, PrivateMethodTester, Status, Suite, TestData}

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.immutable

class SQLDataSourceExampleTest extends QueryTest
  with BeforeAndAfter with PrivateMethodTester with SharedSparkSession with ForAllTestContainer {

  import testImplicits._

  override val container: PostgreSQLContainer = PostgreSQLContainer("postgres:11.8")
  var connection: Connection = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName(container.driverClassName)
    connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
    connection.prepareStatement("create schema test").executeUpdate()
  }

  protected override def afterAll(): Unit = {
    connection.close()
    super.afterAll()
  }

  test("Run JDBC dataset example") {
    //Arrange
    import SQLDataSourceExampleTest._
    val tableName = "test.basic_write_est"
    val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
    val args = Map("input_path" -> getTestResourcePath("test-data.parquet"), "url" -> container.jdbcUrl,
      "dbtable" -> tableName, "user" -> container.username, "password" -> container.password)
      .toSeq
      .map(pair => pair.productIterator.mkString("="))
      .toArray
    //Act
    SQLDataSourceExample.runJdbcDatasetExample(spark, args)
    //Assert
    val connectionProperties = new Properties()
    connectionProperties.put("user", container.username)
    connectionProperties.put("password", container.password)
    assert(2 === spark.read.jdbc(container.jdbcUrl, tableName, connectionProperties).count())
    assert(2 === spark.read.jdbc(container.jdbcUrl, tableName, connectionProperties).collect()(0).length)
  }

  test("Test arguments parsing") {
    //Arrange
    val args = Array("a=1", "b=2")
    //Act
    val result = SQLDataSourceExample.parseArguments(args)
    //Assert
    assert(Map("a" -> "1", "b" -> "2") == result)
  }
}

object SQLDataSourceExampleTest {
  private lazy val data = Array[Row](Row.apply("dave", 42), Row.apply("mary", 222))
  private lazy val schema = StructType(
    StructField("name", StringType) ::
      StructField("id", IntegerType) :: Nil)
}