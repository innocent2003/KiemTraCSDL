package org.example;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;
import org.elasticsearch.common.xcontent.XContentType;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CrudDataApp {
    private static RestHighLevelClient client;

    public static void main(String[] args) {
        // Initialize Elasticsearch client
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

        // Create Swing frame
        JFrame frame = new JFrame("Elasticsearch CRUD App - Java 8");
        frame.setSize(800, 600);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLayout(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5); // Padding

        // ID field
        JLabel labelId = new JLabel("ID:");
        gbc.gridx = 0;
        gbc.gridy = 0;
        frame.add(labelId, gbc);
        JTextField textId = new JTextField(20);
        gbc.gridx = 1;
        frame.add(textId, gbc);

        // Other data fields
        String[] labels = {"switch", "src", "dst", "pktcount", "bytecount", "dur", "dur_nsec", "tot_dur",
                "flows", "packetins", "pktperflow", "byteperflow", "pktrate", "Pairflow",
                "Protocol", "port_no", "tx_bytes", "rx_bytes", "tx_kbps", "rx_kbps", "tot_kbps", "label"};

        JTextField[] textFields = new JTextField[labels.length];

        for (int i = 0; i < labels.length; i++) {
            JLabel label = new JLabel(labels[i] + ":");
            gbc.gridx = 0;
            gbc.gridy = i + 1;
            frame.add(label, gbc);
            textFields[i] = new JTextField(20);
            gbc.gridx = 1;
            frame.add(textFields[i], gbc);
        }

        // CRUD Buttons
        JButton createButton = new JButton("Create");
        gbc.gridx = 0;
        gbc.gridy = labels.length + 1;
        frame.add(createButton, gbc);

        JButton readButton = new JButton("Read");
        gbc.gridx = 1;
        frame.add(readButton, gbc);

        JButton updateButton = new JButton("Update");
        gbc.gridx = 2;
        frame.add(updateButton, gbc);

        JButton deleteButton = new JButton("Delete");
        gbc.gridx = 3;
        frame.add(deleteButton, gbc);

        // Create button action
        createButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String id = textId.getText();
                if (id.isEmpty()) {
                    showError("Please enter a valid ID.");
                    return;
                }

                Map<String, Object> jsonMap = new HashMap<>();
                for (int i = 0; i < labels.length; i++) {
                    jsonMap.put(labels[i], textFields[i].getText());
                }

                IndexRequest request = new IndexRequest("sdn", "doc") // Specify document type
                        .id(id)
                        .source(jsonMap, XContentType.JSON);
                try {
                    client.index(request, RequestOptions.DEFAULT);
                    JOptionPane.showMessageDialog(frame, "Document created.");
                } catch (IOException ex) {
                    showError("Failed to create document: " + ex.getMessage());
                }
            }
        });

        // Read button action
        readButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String id = textId.getText();
                if (id.isEmpty()) {
                    showError("Please enter a valid ID.");
                    return;
                }

                GetRequest getRequest = new GetRequest("sdn").id(id);
                try {
                    GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
                    if (response.isExists()) {
                        Map<String, Object> source = response.getSource();
                        for (int i = 0; i < labels.length; i++) {
                            textFields[i].setText((String) source.get(labels[i]));
                        }
                        JOptionPane.showMessageDialog(frame, "Document retrieved with ID: " + id);
                    } else {
                        showError("Document not found with ID: " + id);
                    }
                } catch (IOException ex) {
                    showError("Failed to retrieve document: " + ex.getMessage());
                }
            }
        });

        // Update button action
        updateButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String id = textId.getText();
                if (id.isEmpty()) {
                    showError("Please enter a valid ID.");
                    return;
                }

                Map<String, Object> jsonMap = new HashMap<>();
                for (int i = 0; i < labels.length; i++) {
                    jsonMap.put(labels[i], textFields[i].getText());
                }

                UpdateRequest request = new UpdateRequest("sdn", "doc", id).doc(jsonMap);// Correct usage
                try {
                    client.update(request, RequestOptions.DEFAULT);
                    JOptionPane.showMessageDialog(frame, "Document updated with ID: " + id);
                } catch (IOException ex) {
                    showError("Failed to update document: " + ex.getMessage());
                }
            }
        });

        // Delete button action
        deleteButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String id = textId.getText();
                if (id.isEmpty()) {
                    showError("Please enter a valid ID.");
                    return;
                }
//                DeleteRequest request = new DeleteRequest("sdn", "your_doc_type", id).doc(jsonMap);
                DeleteRequest request = new DeleteRequest("sdn", "doc", id);
                try {
                    client.delete(request, RequestOptions.DEFAULT);
                    JOptionPane.showMessageDialog(frame, "Document deleted with ID: " + id);
                } catch (IOException ex) {
                    showError("Failed to delete document: " + ex.getMessage());
                }
            }
        });

        // Show frame
        frame.setVisible(true);

        // Add shutdown hook to close Elasticsearch client
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // Helper method to show error messages
    private static void showError(String message) {
        JOptionPane.showMessageDialog(null, message, "Error", JOptionPane.ERROR_MESSAGE);
    }
}
