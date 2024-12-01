import org.apache.spark.sql.SparkSession;

public class IcebergExample {

    public static void main(String[] args) {
        // Initialize SparkSession with Iceberg support
        SparkSession spark = SparkSession.builder()
                .appName("IcebergExample")
                .master("local[*]")
                .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.my_catalog.type", "hadoop")
                .config("spark.sql.catalog.my_catalog.warehouse", "file:///D:/workdir/practice/iceberg-spark/warehouse")
                .getOrCreate();

        // Example: Create and query an Iceberg table
        spark.sql("CREATE TABLE my_catalog.db.test_table (id INT, name STRING) USING iceberg");
        spark.sql("INSERT INTO my_catalog.db.test_table VALUES (1, 'Alice'), (2, 'Bob')");
        spark.sql("SELECT * FROM my_catalog.db.test_table").show();

        // Stop the Spark session
        spark.stop();

    }

}