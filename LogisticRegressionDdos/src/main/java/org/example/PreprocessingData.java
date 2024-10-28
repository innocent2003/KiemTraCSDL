package org.example;

import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.VectorAssembler;

public class PreprocessingData {
    public static void main(String[] args) {
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Categorical Encoding Example")
                .master("local[*]") // Use local for testing
                .getOrCreate();

        // Load CSV data into DataFrame
        String csvFilePath = "G:\\bigdataPJ\\dataset_sdn.csv"; // Update your path
        Dataset<Row> data = spark.read()
                .option("header", "true") // Use first line as header
                .option("inferSchema", "true") // Infer data types
                .csv(csvFilePath);

        // Display the original DataFrame
        System.out.println("Original DataFrame:");
        data.show();

        // String Indexer for a categorical column (e.g., "Protocol")
        StringIndexer indexer = new StringIndexer()
                .setInputCol("Protocol")
                .setOutputCol("Protocol_Index");

        // Fit the indexer to the data and transform it
        StringIndexerModel indexerModel = indexer.fit(data);
        Dataset<Row> indexedData = indexerModel.transform(data);

        // One-Hot Encoding for the indexed column
        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("Protocol_Index")
                .setOutputCol("Protocol_OHE");

        // Fit and transform using OneHotEncoder
        Dataset<Row> encodedData = encoder.fit(indexedData).transform(indexedData);

        // Assemble features into a single vector (optional, for ML models)
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount"}) // Add other columns as needed
                .setOutputCol("features");

        Dataset<Row> finalData = assembler.transform(encodedData);

        // Show the final DataFrame
        System.out.println("Final DataFrame with Encoded Features:");
        finalData.select("features").show();

        // Stop Spark Session
        spark.stop();
    }
}
