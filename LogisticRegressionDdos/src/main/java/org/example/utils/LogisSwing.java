package org.example.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.LinearSVC; // Change to LinearSVC for SVM
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexerModel;

import java.util.Collections;
import java.util.List;

public class LogisSwing {
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

        // Index and encode categorical columns (e.g., "Protocol")
        StringIndexer indexer = new StringIndexer()
                .setInputCol("Protocol")
                .setOutputCol("Protocol_Index");

        StringIndexerModel indexerModel = indexer.fit(data);
        Dataset<Row> indexedData = indexerModel.transform(data);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("Protocol_Index")
                .setOutputCol("Protocol_OHE");

        Dataset<Row> encodedData = encoder.fit(indexedData).transform(indexedData);

        // Vector Assembler to combine all features into a single feature vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "tot_dur", "flows", "packetins", "byteperflow", "tx_bytes", "rx_bytes", "tx_kbps", "rx_kbps", "tot_kbps"}) // Include columns you want as features
                .setOutputCol("features");

        Dataset<Row> finalData = assembler.transform(encodedData)
                .withColumnRenamed("label", "label"); // Rename label column if necessary

        // Split the data into training and test sets
        Dataset<Row>[] splitData = finalData.randomSplit(new double[]{0.7, 0.3}, 12345);
        Dataset<Row> trainingData = splitData[0];
        Dataset<Row> testData = splitData[1];

        // Initialize SVM model
        LinearSVC svm = new LinearSVC()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setMaxIter(10);

        // Train the model
        LinearSVCModel svmModel = svm.fit(trainingData);

        // Make predictions on test data
        Dataset<Row> predictions = svmModel.transform(testData);

        // Evaluate the model using Binary Classification Evaluator
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("rawPrediction");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Model accuracy: " + accuracy);

        // Display prediction results
        predictions.select("features", "label", "prediction", "probability").show();

        // Stop Spark session
        spark.stop();
    }

    private static Dataset<Row> createInputData(SparkSession spark, String protocol, double pktcount, double bytecount, double totDur, double flows, double packetins, double byteperflow, double txBytes, double rxBytes, double txKbps, double rxKbps, double totKbps) {
        // Create a DataFrame with the input data
        String inputData = String.format("%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f", protocol, pktcount, bytecount, totDur, flows, packetins, byteperflow, txBytes, rxBytes, txKbps, rxKbps, totKbps);
        String csvData = "Protocol,pktcount,bytecount,tot_dur,flows,packetins,byteperflow,tx_bytes,rx_bytes,tx_kbps,rx_kbps,tot_kbps\n" + inputData;

        // Use a List to hold the input data
        List<String> data = Collections.singletonList(csvData);

        // Create a temporary DataFrame from the input data
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(spark.createDataset(data, Encoders.STRING())); // Use Encoders.STRING()
    }
}
