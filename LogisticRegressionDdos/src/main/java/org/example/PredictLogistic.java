package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class PredictLogistic {

    public static void main(String[] args) throws IOException {

        // Step 1: Set up Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Logistic Regression Example")
                .master("local") // Change to your cluster master URL in production
                .getOrCreate();

        // Step 2: Load data
        // Replace "path/to/your/data.csv" with your actual file path
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("G:\\bigdataPJ\\ProjectCode\\dataset_sdn.csv");

        // Step 3: Preprocess data
        // Assuming columns to use for features are in featureColumns array
        String[] featureColumns = new String[] {
                "switch", "pktcount", "bytecount", "dur_nsec",
                "tot_dur", "flows", "packetins", "byteperflow",
                "Pairflow", "port_no", "tx_bytes", "rx_bytes",
                "tx_kbps", "rx_kbps", "tot_kbps"
        };

        // Assemble features into a single vector column
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureColumns)
                .setOutputCol("features");

        // Apply the assembler and cast label column to DoubleType
        Dataset<Row> preparedData = assembler.transform(data)
                .withColumn("label", col("label").cast(DataTypes.DoubleType));

        // Step 4: Split data into training and test sets
        Dataset<Row>[] splits = preparedData.randomSplit(new double[]{0.7, 0.3}, 1234);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Step 5: Train the Logistic Regression model
        LogisticRegression logisticRegression = new LogisticRegression()
                .setLabelCol("label")
                .setFeaturesCol("features");

        LogisticRegressionModel model = logisticRegression.fit(trainingData);

        // Step 6: Make predictions on the test data
        Dataset<Row> predictions = model.transform(testData);

        // Display the predictions
        predictions.select("features", "label", "prediction", "probability").show();

        // Step 7: Evaluate the model (optional)
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("prediction")
                .setMetricName("areaUnderROC");

        double auc = evaluator.evaluate(predictions);
        System.out.println("Area under ROC = " + auc);

        // Step 8: Save the trained model (optional)
        // Replace "path/to/save/model" with the desired path for saving the model
        model.save("G:\\bigdataPJ\\ProjectCode\\dataset_sdnb.csv");

        // Stop Spark session
        spark.stop();
    }
}
