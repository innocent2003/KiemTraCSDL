package org.example;

import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

public class SVMInputPredictor extends JFrame {

    private JTextField protocolField, pktcountField, bytecountField, dur_nsecField, tot_durField, flowsField,
            packetinsField, byteperflowField, tx_bytesField, rx_bytesField, tx_kbpsField, rx_kbpsField, tot_kbpsField;
    private JLabel predictionLabel;
    private LinearSVCModel model;
    private SparkSession spark;

    public SVMInputPredictor(LinearSVCModel model, SparkSession spark) {
        this.model = model;
        this.spark = spark;

        setTitle("SVM Predictor");
        setLayout(new GridLayout(16, 2));

        // Create input fields for each feature
        add(new JLabel("Protocol (Index):"));
        protocolField = new JTextField();
        add(protocolField);

        add(new JLabel("Packet Count:"));
        pktcountField = new JTextField();
        add(pktcountField);

        add(new JLabel("Byte Count:"));
        bytecountField = new JTextField();
        add(bytecountField);

        add(new JLabel("Duration (ns):"));
        dur_nsecField = new JTextField();
        add(dur_nsecField);

        add(new JLabel("Total Duration:"));
        tot_durField = new JTextField();
        add(tot_durField);

        add(new JLabel("Flows:"));
        flowsField = new JTextField();
        add(flowsField);

        add(new JLabel("Packet Ins:"));
        packetinsField = new JTextField();
        add(packetinsField);

        add(new JLabel("Byte Per Flow:"));
        byteperflowField = new JTextField();
        add(byteperflowField);

        add(new JLabel("TX Bytes:"));
        tx_bytesField = new JTextField();
        add(tx_bytesField);

        add(new JLabel("RX Bytes:"));
        rx_bytesField = new JTextField();
        add(rx_bytesField);

        add(new JLabel("TX kbps:"));
        tx_kbpsField = new JTextField();
        add(tx_kbpsField);

        add(new JLabel("RX kbps:"));
        rx_kbpsField = new JTextField();
        add(rx_kbpsField);

        add(new JLabel("Total kbps:"));
        tot_kbpsField = new JTextField();
        add(tot_kbpsField);

        // Add a button to make predictions
        JButton predictButton = new JButton("Predict");
        predictButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                makePrediction();
            }
        });
        add(predictButton);

        // Label to display the prediction result
        predictionLabel = new JLabel("Prediction: ");
        add(predictionLabel);

        setSize(400, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);
    }

    private void makePrediction() {
        try {
            // Collect input values and convert them to double
            double protocol = Double.parseDouble(protocolField.getText());
            double pktcount = Double.parseDouble(pktcountField.getText());
            double bytecount = Double.parseDouble(bytecountField.getText());
            double dur_nsec = Double.parseDouble(dur_nsecField.getText());
            double tot_dur = Double.parseDouble(tot_durField.getText());
            double flows = Double.parseDouble(flowsField.getText());
            double packetins = Double.parseDouble(packetinsField.getText());
            double byteperflow = Double.parseDouble(byteperflowField.getText());
            double tx_bytes = Double.parseDouble(tx_bytesField.getText());
            double rx_bytes = Double.parseDouble(rx_bytesField.getText());
            double tx_kbps = Double.parseDouble(tx_kbpsField.getText());
            double rx_kbps = Double.parseDouble(rx_kbpsField.getText());
            double tot_kbps = Double.parseDouble(tot_kbpsField.getText());

            // Assemble feature vector
            Vector features = Vectors.dense(protocol, pktcount, bytecount, dur_nsec, tot_dur, flows, packetins, byteperflow, tx_bytes, rx_bytes, tx_kbps, rx_kbps, tot_kbps);

            // Make prediction using the model
            double prediction = model.predict(features);

            // Display the result
            predictionLabel.setText("Prediction: " + prediction);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Invalid input! Please enter valid numbers.");
        }
    }

    public static void main(String[] args) {
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
                .withColumnRenamed("label", "label");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Protocol_OHE", "pktcount", "bytecount", "dur_nsec", "tot_dur",
                        "flows", "packetins", "byteperflow", "tx_bytes", "rx_bytes",
                        "tx_kbps", "rx_kbps", "tot_kbps"})
                .setOutputCol("features");

        Dataset<Row> assembledData = assembler.transform(finalData);

        // Split the data
        Dataset<Row>[] splitData = assembledData.randomSplit(new double[]{0.7, 0.3}, 12345);
        Dataset<Row> trainingData = splitData[0];

        LinearSVCModel model;
        String modelPath = "G:\\bigdataPJ\\ProjectCode\\svmModel";

        try {
            model = LinearSVCModel.load(modelPath);
        } catch (Exception e) {
            LinearSVC svm = new LinearSVC()
                    .setLabelCol("label")
                    .setFeaturesCol("features")
                    .setMaxIter(10);
            model = svm.fit(trainingData);
//            model.save(modelPath);
        }

        // Create the Swing application
        new SVMInputPredictor(model, spark);
    }
}
