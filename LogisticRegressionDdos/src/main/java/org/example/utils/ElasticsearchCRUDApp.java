package org.example.utils;

import javax.swing.*;
import java.io.IOException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;

public class ElasticsearchCRUDApp {
    private JTextField indexField;
    private JTextField idField;
    private JTextArea jsonArea;
    private JButton createButton;
    private JButton readButton;
    private JButton updateButton;
    private JButton deleteButton;

    public ElasticsearchCRUDApp() {
        JFrame frame = new JFrame("Elasticsearch CRUD");
        frame.setSize(400, 300);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel panel = new JPanel();
        frame.add(panel);
        placeComponents(panel);
        frame.setVisible(true);
    }

    private void placeComponents(JPanel panel) {
        panel.setLayout(null);

        JLabel indexLabel = new JLabel("Index:");
        indexLabel.setBounds(10, 20, 80, 25);
        panel.add(indexLabel);

        indexField = new JTextField(20);
        indexField.setBounds(100, 20, 165, 25);
        panel.add(indexField);

        JLabel idLabel = new JLabel("ID:");
        idLabel.setBounds(10, 50, 80, 25);
        panel.add(idLabel);

        idField = new JTextField(20);
        idField.setBounds(100, 50, 165, 25);
        panel.add(idField);

        JLabel jsonLabel = new JLabel("JSON:");
        jsonLabel.setBounds(10, 80, 80, 25);
        panel.add(jsonLabel);

        jsonArea = new JTextArea();
        jsonArea.setBounds(100, 80, 165, 75);
        panel.add(jsonArea);

        createButton = new JButton("Create");
        createButton.setBounds(10, 160, 80, 25);
        panel.add(createButton);

        readButton = new JButton("Read");
        readButton.setBounds(100, 160, 80, 25);
        panel.add(readButton);

        updateButton = new JButton("Update");
        updateButton.setBounds(190, 160, 80, 25);
        panel.add(updateButton);

        deleteButton = new JButton("Delete");
        deleteButton.setBounds(280, 160, 80, 25);
        panel.add(deleteButton);

        createButton.addActionListener(e -> {
            try {
                createDocument(indexField.getText(), idField.getText(), jsonArea.getText());
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });

        readButton.addActionListener(e -> {
            try {
                readDocument(indexField.getText(), idField.getText());
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });

        updateButton.addActionListener(e -> {
            try {
                updateDocument(indexField.getText(), idField.getText(), jsonArea.getText());
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });

        deleteButton.addActionListener(e -> {
            try {
                deleteDocument(indexField.getText(), idField.getText());
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });
    }

    public void createDocument(String index, String id, String jsonString) throws IOException {
        // Add a type name, for example "product"
        IndexRequest request = new IndexRequest(index, "product", id).source(jsonString, XContentType.JSON);
        IndexResponse response = ElasticsearchClient.getClient().index(request, RequestOptions.DEFAULT);
        System.out.println("Document created: " + response.getId());
    }

    public void readDocument(String index, String id) throws IOException {
        // Specify the type, for example "product"
        GetRequest request = new GetRequest(index, "product", id);
        GetResponse response = ElasticsearchClient.getClient().get(request, RequestOptions.DEFAULT);

        if (response.isExists()) {
            System.out.println("Document found: " + response.getSourceAsString());
            jsonArea.setText(response.getSourceAsString());
        } else {
            System.out.println("Document not found");
            jsonArea.setText("Document not found");
        }
    }

    public void updateDocument(String index, String id, String jsonString) throws IOException {
        // Specify the type, for example "product"
        UpdateRequest request = new UpdateRequest(index, "product", id).doc(jsonString, XContentType.JSON);
        UpdateResponse response = ElasticsearchClient.getClient().update(request, RequestOptions.DEFAULT);
        System.out.println("Document updated: " + response.getId());
    }

    public void deleteDocument(String index, String id) throws IOException {
        // Specify the type, for example "product"
        DeleteRequest request = new DeleteRequest(index, "product", id);
        DeleteResponse response = ElasticsearchClient.getClient().delete(request, RequestOptions.DEFAULT);
        System.out.println("Document deleted: " + response.getId());
    }

    public static void main(String[] args) {
        new ElasticsearchCRUDApp();
    }
}
