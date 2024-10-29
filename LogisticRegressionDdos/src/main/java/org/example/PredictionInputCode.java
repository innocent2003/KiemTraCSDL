package org.example;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.RowFactory;

import java.util.Arrays;
import java.util.Scanner;
public class PredictionInputCode {
    public static void main(String[] args) {

        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Logistic Regression Example")
                .master("local[*]") // Use all available cores
                .getOrCreate();

        // Load CSV data into DataFrame
        String csvFilePath = "G:\\bigdataPJ\\ProjectCode\\dataset_sdn.csv"; // Update with your CSV file path
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvFilePath);

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
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "pktperflow", "tx_bytes", "rx_bytes"})
                .setOutputCol("features");
        Dataset<Row> finalData = assembler.transform(encodedData)
                .withColumnRenamed("label", "label");

        // Split the data into training and test sets
        Dataset<Row>[] splitData = finalData.randomSplit(new double[]{0.7, 0.3}, 12345);
        Dataset<Row> trainingData = splitData[0];

        // Initialize Logistic Regression model and train
        LogisticRegression logisticRegression = new LogisticRegression()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setMaxIter(10);
        LogisticRegressionModel model = logisticRegression.fit(trainingData);

        // Accept input data from keyboard
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter Protocol Index: ");
        double protocolIndex = scanner.nextDouble();
        System.out.println("Enter pktcount: ");
        double pktcount = scanner.nextDouble();
        System.out.println("Enter bytecount: ");
        double bytecount = scanner.nextDouble();
        System.out.println("Enter pktperflow: ");
        double pktperflow = scanner.nextDouble();
        System.out.println("Enter tx_bytes: ");
        double tx_bytes = scanner.nextDouble();
        System.out.println("Enter rx_bytes: ");
        double rx_bytes = scanner.nextDouble();

        // Prepare user input data as a DataFrame
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("Protocol_OHE", DataTypes.DoubleType, false),
                DataTypes.createStructField("pktcount", DataTypes.DoubleType, false),
                DataTypes.createStructField("bytecount", DataTypes.DoubleType, false),
                DataTypes.createStructField("pktperflow", DataTypes.DoubleType, false),
                DataTypes.createStructField("tx_bytes", DataTypes.DoubleType, false),
                DataTypes.createStructField("rx_bytes", DataTypes.DoubleType, false)
        });

        Row inputData = RowFactory.create(protocolIndex, pktcount, bytecount, pktperflow, tx_bytes, rx_bytes);
        Dataset<Row> inputDF = spark.createDataFrame(Arrays.asList(inputData), schema);

        // Transform user input with VectorAssembler
        Dataset<Row> assembledInput = assembler.transform(inputDF);

        // Make predictions
        Dataset<Row> predictions = model.transform(assembledInput);
        predictions.select("features", "prediction", "probability").show();

        // Stop Spark session
        spark.stop();
        scanner.close();
    }
}
