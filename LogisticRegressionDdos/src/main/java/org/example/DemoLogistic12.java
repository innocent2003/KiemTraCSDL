package org.example;



import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

public class DemoLogistic12 {

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

        // Handle nulls by filling them with 0 or an appropriate value
        data = data.na().fill(0);

        // Index and encode categorical columns (e.g., "Protocol")
        StringIndexer indexer = new StringIndexer()
                .setInputCol("Protocol")
                .setOutputCol("Protocol_Index")
                .setHandleInvalid("skip"); // Skip rows with null values in "Protocol"

        Dataset<Row> indexedData = indexer.fit(data).transform(data);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("Protocol_Index")
                .setOutputCol("Protocol_OHE");

        Dataset<Row> encodedData = encoder.fit(indexedData).transform(indexedData);

        // Convert each feature column to DoubleType
        Dataset<Row> finalData = encodedData
                .withColumn("pktcount", functions.col("pktcount").cast(DataTypes.DoubleType))
                .withColumn("bytecount", functions.col("bytecount").cast(DataTypes.DoubleType))
                .withColumn("dur_nsec", functions.col("dur_nsec").cast(DataTypes.DoubleType))
                .withColumn("tot_dur", functions.col("tot_dur").cast(DataTypes.DoubleType))
                .withColumn("flows", functions.col("flows").cast(DataTypes.DoubleType))
                .withColumn("packetins", functions.col("packetins").cast(DataTypes.DoubleType))
                .withColumn("byteperflow", functions.col("byteperflow").cast(DataTypes.DoubleType))
                .withColumn("tx_bytes", functions.col("tx_bytes").cast(DataTypes.DoubleType))
                .withColumn("rx_bytes", functions.col("rx_bytes").cast(DataTypes.DoubleType))
                .withColumn("tx_kbps", functions.col("tx_kbps").cast(DataTypes.DoubleType))
                .withColumn("rx_kbps", functions.col("rx_kbps").cast(DataTypes.DoubleType))
                .withColumn("tot_kbps", functions.col("tot_kbps").cast(DataTypes.DoubleType))
                .withColumnRenamed("label", "label"); // Rename label column if necessary

        // Vector Assembler to combine all features into a single feature vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "dur_nsec", "tot_dur",
                        "flows", "packetins", "byteperflow", "tx_bytes", "rx_bytes",
                        "tx_kbps", "rx_kbps", "tot_kbps"})
                .setOutputCol("features");

        Dataset<Row> assembledData = assembler.transform(finalData);

        // Split the data into training and test sets
        Dataset<Row>[] splitData = assembledData.randomSplit(new double[]{0.7, 0.3}, 12345);
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
                .setRawPredictionCol("rawPrediction");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Model accuracy: " + accuracy);

        // Display prediction results
        predictions.select("features", "label", "prediction", "rawPrediction").show();

        // Stop Spark session
        spark.stop();
        }
}

