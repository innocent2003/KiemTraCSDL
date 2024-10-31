package org.example;

import com.opencsv.CSVReader; // Importing OpenCSV's CSVReader
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.example.utils.SVMInputPredictor;

import javax.swing.*;
import java.io.File;
import java.io.FileReader; // Importing FileReader for file operations
import java.io.IOException; // Importing IOException for handling file errors

public class Main {

    public static void main(String[] args) throws IOException {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("SVM Example")
                .master("local[*]") // Use all available cores
                .getOrCreate();

        // Load CSV data
        String csvFilePath = "G:\\bigdataPJ\\ProjectCode\\dataset_sdn.csv";
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvFilePath);

        // Data preprocessing steps
        data = data.na().fill(0);

        StringIndexer indexer = new StringIndexer()
                .setInputCol("Protocol")
                .setOutputCol("Protocol_Index")
                .setHandleInvalid("skip");

        Dataset<Row> indexedData = indexer.fit(data).transform(data);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("Protocol_Index")
                .setOutputCol("Protocol_OHE");

        Dataset<Row> encodedData = encoder.fit(indexedData).transform(indexedData);

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
                .withColumn("switch",functions.col("switch").cast(DataTypes.DoubleType))
                .withColumn("label", functions.col("label").cast(DataTypes.DoubleType));

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "dur_nsec", "tot_dur",
                        "flows", "packetins", "byteperflow", "tx_bytes", "rx_bytes",
                        "tx_kbps", "rx_kbps", "tot_kbps"})
                .setOutputCol("features");

        Dataset<Row> assembledData = assembler.transform(finalData).select("features", "label");

        // Model training
        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(10)
                .setRegParam(0.1);

        String modelPath = "G:\\bigdataPJ\\ProjectCode\\svm_model";
        LinearSVCModel model;

        if (new File(modelPath).exists()) {
            model = LinearSVCModel.load(modelPath);
        } else {
            model = lsvc.fit(assembledData);
            model.save(modelPath);
        }

        // Launch GUI
        SwingUtilities.invokeLater(() -> new SVMInputPredictor(model, spark));
    }
}
