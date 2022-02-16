package example

import org.apache.spark.sql.SparkSession
import io.delta.tables._

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

object QuickstartSQL {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder()
        .appName("QuickstartSQL")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    
        val tableName = "tblname"

        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"DROP TABLE IF EXISTS newData")

        try {
        // Create a table
        println("Creating a table")
        spark.sql(s"CREATE TABLE $tableName(id LONG) USING delta")
        spark.sql(s"INSERT INTO $tableName VALUES 0, 1, 2, 3, 4")

        // Read table
        println("Reading the table")
        spark.sql(s"SELECT * FROM $tableName").show()

        // Upsert (merge) new data
        println("Upsert new data")
        spark.sql("CREATE TABLE newData(id LONG) USING parquet")
        spark.sql("INSERT INTO newData VALUES 3, 4, 5, 6")
        
        spark.sql(s"""MERGE INTO $tableName USING newData
            ON ${tableName}.id = newData.id
            WHEN MATCHED THEN
                UPDATE SET ${tableName}.id = newData.id
            WHEN NOT MATCHED THEN INSERT *
        """)

        spark.sql(s"SELECT * FROM $tableName").show()

        // Update table data
        println("Overwrite the table")
        spark.sql(s"INSERT OVERWRITE $tableName VALUES 5, 6, 7, 8, 9")
        spark.sql(s"SELECT * FROM $tableName").show()

        // Update every even value by adding 100 to it
        println("Update to the table (add 100 to every even value)")
        spark.sql(s"UPDATE $tableName SET id = (id + 100) WHERE (id % 2 == 0)")
        spark.sql(s"SELECT * FROM $tableName").show()

        // Delete every even value
        spark.sql(s"DELETE FROM $tableName WHERE (id % 2 == 0)")
        spark.sql(s"SELECT * FROM $tableName").show()

        // Read old version of the data using time travel
        print("Read old data using time travel")
        val df2 = spark.read.format("delta").option("versionAsOf", 0).table(tableName)
        df2.show()
        } finally {
        // Cleanup
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(s"DROP TABLE IF EXISTS newData")
        spark.stop()
        }
    }
}