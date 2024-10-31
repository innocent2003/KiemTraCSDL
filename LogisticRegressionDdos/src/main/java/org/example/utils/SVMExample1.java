package org.example.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexerModel;

public class SVMExample1 {
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
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "pktperflow", "tx_bytes", "rx_bytes"}) // Include columns you want as features
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
        LinearSVCModel model = svm.fit(trainingData);

        // Make predictions on test data
        Dataset<Row> predictions = model.transform(testData);

        // Evaluate the model using Binary Classification Evaluator
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("prediction"); // Use 'prediction' for evaluation

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Model accuracy: " + accuracy);

        // Display prediction results
        predictions.select("features", "label", "prediction", "probability").show();

        // Stop Spark session
        spark.stop();
    }
}
