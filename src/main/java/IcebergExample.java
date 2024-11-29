import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class IcebergExample {

    public static void main(String[] args) {
        // Initialize SparkSession with Iceberg support
        SparkSession spark = SparkSession.builder()
                .appName("IcebergExample")
                .master("local[*]")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://hadoop-namenode:9000/warehouse")
                .getOrCreate();

        // Define the schema for the table
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );

        // Create the table if it does not exist

        TableIdentifier tableIdentifier = TableIdentifier.of("default", "sample_table");
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", "hdfs://hadoop-namenode:9000/warehouse");
        Catalog catalog = CatalogUtil.loadCatalog(
                "org.apache.iceberg.hadoop.HadoopCatalog",
                "hadoop_catalog",
                properties,
                spark.sparkContext().hadoopConfiguration()
        );


        Table table;
        if (!catalog.tableExists(tableIdentifier)) {
            table = catalog.createTable(tableIdentifier, schema);
        } else {
            table = catalog.loadTable(tableIdentifier);
        }

        // Create a sample DataFrame
        Dataset<Row> data = spark.createDataFrame(
                java.util.Arrays.asList(
                        new Person(1, "Alice"),
                        new Person(2, "Bob")
                ),
                Person.class
        );

        // Write data to Iceberg table
        data.write()
                .format("iceberg")
                .mode(SaveMode.Append)
                .save("hadoop_catalog.default.sample_table");

        System.out.println("Table created and data written!");
    }

}
