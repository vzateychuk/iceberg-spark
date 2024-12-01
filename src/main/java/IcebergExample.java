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
                .appName("IcebergExample")
                .master("local[*]")
                .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.my_catalog.type", "hadoop")
                .config("spark.sql.catalog.my_catalog.warehouse", "file:///D:/workdir/practice/iceberg-spark/warehouse")
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
/*
                        new Person(1, "Alice"),
                        new Person(2, "Bob")
*/
                        new Person(3, "VZateychuk")
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
