package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.LinearSVC; // Import LinearSVC for SVM
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexerModel;

public class SVMExample {
    public static void main(String[] args) {

        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("SVM Example")
                .master("local[*]") // Use all available cores
                .getOrCreate();

        // Load CSV data into DataFrame
        String csvFilePath = "G:\\bigdataPJ\\ProjectCode\\dataset_sdn.csv"; // Update with your CSV file path
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvFilePath);

        // Display the original DataFrame
        System.out.println("Original DataFrame:");
        data.show();

        // Check if 'label' is numeric and convert if necessary
        StringIndexer labelIndexer = new StringIndexer()
                .setInputCol("label") // Assuming 'label' column exists in your data
                .setOutputCol("indexedLabel");

        StringIndexerModel labelIndexerModel = labelIndexer.fit(data);
        Dataset<Row> indexedData = labelIndexerModel.transform(data);

        // Index and encode categorical columns (e.g., "Protocol")
        StringIndexer indexer = new StringIndexer()
                .setInputCol("Protocol")
                .setOutputCol("Protocol_Index");

        StringIndexerModel indexerModel = indexer.fit(indexedData);
        Dataset<Row> indexedProtocolData = indexerModel.transform(indexedData);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("Protocol_Index")
                .setOutputCol("Protocol_OHE");

        Dataset<Row> encodedData = encoder.fit(indexedProtocolData).transform(indexedProtocolData);

        // Vector Assembler to combine all features into a single feature vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "flows", "tx_bytes", "rx_bytes"}) // Make sure all columns are correct
                .setOutputCol("features");

        Dataset<Row> finalData = assembler.transform(encodedData)
                .select("features", "indexedLabel"); // Select features and the new indexed label

        // Split the data into training and test sets
        Dataset<Row>[] splitData = finalData.randomSplit(new double[]{0.7, 0.3}, 12345);
        Dataset<Row> trainingData = splitData[0];
        Dataset<Row> testData = splitData[1];

        // Initialize SVM model
        LinearSVC svm = new LinearSVC()
                .setLabelCol("indexedLabel") // Set the indexed label column
                .setFeaturesCol("features") // Set the features column
                .setMaxIter(10) // Maximum number of iterations
                .setRegParam(0.1); // Regularization parameter

        // Train the model
        LinearSVCModel model = svm.fit(trainingData);

        // Make predictions on test data
        Dataset<Row> predictions = model.transform(testData);

        // Evaluate the model using Binary Classification Evaluator
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("indexedLabel") // Use the indexed label for evaluation
                .setRawPredictionCol("rawPrediction");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Model accuracy: " + accuracy);

        // Display prediction results
        predictions.select("features", "indexedLabel", "prediction").show(); // Removed probability column

        // Stop Spark session
        spark.stop();
    }
}
