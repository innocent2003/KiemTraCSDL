package org.example;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionExample {
    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("LogisticRegressionExample")
                .master("local[*]")
                .getOrCreate();

        // Load the CSV dataset
        Dataset<Row> data = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("G:\\bigdataPJ\\dataset_sdn.csv");

        // Select features and label column for model training
        // Modify feature columns based on the dataset columns
        String[] featureCols = {"dt", "switch", "pktcount", "bytecount", "dur", "dur_nsec",
                "tot_dur", "flows", "packetins", "pktperflow", "byteperflow",
                "pktrate", "port_no", "tx_bytes", "rx_bytes", "tx_kbps",
                "rx_kbps", "tot_kbps"};

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features");

        // Transform data to add features vector
        Dataset<Row> transformedData = assembler.transform(data).select("features", "label");

        // Split the data into training and test sets (80% training, 20% test)
        Dataset<Row>[] splits = transformedData.randomSplit(new double[]{0.8, 0.2}, 1234);
        Dataset<Row> trainData = splits[0];
        Dataset<Row> testData = splits[1];

        // Initialize and train the logistic regression model
        LogisticRegression lr = new LogisticRegression()
                .setLabelCol("label")
                .setFeaturesCol("features");

        LogisticRegressionModel model = lr.fit(trainData);

        // Make predictions on the test data
        Dataset<Row> predictions = model.transform(testData);

        // Evaluate the model's accuracy
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Accuracy = " + accuracy);

        // Stop Spark session
        spark.stop();
    }
}
