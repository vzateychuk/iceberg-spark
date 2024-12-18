import org.apache.spark.sql.SparkSession;

public class IcebergExample {

    public static void main(String[] args) {
        // Initialize SparkSession with Iceberg support
        SparkSession spark = SparkSession.builder()
                .appName("Spark Iceberg with JDBC Catalog")
                .config("spark.master", "local[*]")

                // Iceberg Catalog Configuration for JDBC
                .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")

                // JDBC URI for PostgreSQL
                .config("spark.sql.catalog.my_catalog.uri", "jdbc:postgresql://localhost:5435/catalog")

                // Credentials for PostgreSQL
                .config("spark.sql.catalog.my_catalog.jdbc.user", "admin")
                .config("spark.sql.catalog.my_catalog.jdbc.password", "password")

                // PostgreSQL JDBC Driver
                .config("spark.sql.catalog.my_catalog.jdbc.driver", "org.postgresql.Driver")

                // S3 MinIO Configuration
                .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
                .config("spark.hadoop.fs.s3.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
                .config("spark.hadoop.fs.s3.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
                .config( "spark.hadoop.fs.s3.path.style.access", "true")
                .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

                // Warehouse location on S3 MinIO
                .config("spark.sql.catalog.my_catalog.warehouse", "s3://warehouse/")

                .getOrCreate();
        // Example: Create and query an Iceberg table
        spark.sql("CREATE TABLE my_catalog.db.test_table (id INT, name STRING) USING iceberg");
        spark.sql("INSERT INTO my_catalog.db.test_table VALUES (1, 'Alice'), (2, 'Bob')");
        spark.sql("SELECT * FROM my_catalog.db.test_table").show();

        // Stop the Spark session
        spark.stop();

    }

}