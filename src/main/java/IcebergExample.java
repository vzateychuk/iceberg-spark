import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;

public class IcebergExample {

    public static void main(String[] args) throws TableAlreadyExistsException, NoSuchTableException {

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
                .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/")

                .getOrCreate();

        // Create the table if it does not exist
        TableIdentifier tableIdentifier = TableIdentifier.of("db", "sample_table");
        SparkCatalog sparkCatalog = (SparkCatalog) spark.sessionState().catalogManager().catalog("my_catalog");
        Identifier identifier = Identifier.of(tableIdentifier.namespace().levels(), tableIdentifier.name());
        if (!sparkCatalog.tableExists(identifier)) {
            StructType sparkSchema = new StructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.IntegerType, true),
                    DataTypes.createStructField("name", DataTypes.StringType, true)
            });
            sparkCatalog.createTable(identifier, sparkSchema, null, new HashMap<>());
        } else {
            sparkCatalog.loadTable(identifier);
        }

        // Create a sample DataFrame
        Dataset<Row> data = spark.createDataFrame(
                java.util.Arrays.asList(
                        new Person(4, "Alice-2"),
                        new Person(5, "Bob-2"),
                        new Person(6, "VZateychuk-2")
                ),
                Person.class
        );

        // Write data to Iceberg table
        data.write()
                .format("iceberg")
                .mode(SaveMode.Overwrite)
                .save("my_catalog.db.sample_table");

        System.out.println("Table created and data written!");
    }

}
