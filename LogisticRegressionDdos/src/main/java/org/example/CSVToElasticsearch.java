package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileReader;
import java.io.IOException;

public class CSVToElasticsearch {
    private static final String INDEX_NAME = "sdn"; // Update with your index name
    private RestHighLevelClient client;

    public CSVToElasticsearch(RestHighLevelClient client) {
        this.client = client;
    }

public void importCSV(String csvFilePath) {
    try (FileReader reader = new FileReader(csvFilePath);
         CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader())) {

        for (CSVRecord csvRecord : csvParser) {
            // Define the document type (e.g., "doc" or any other name you choose)
            String documentType = "doc";

            IndexRequest indexRequest = new IndexRequest(INDEX_NAME, documentType)
                    .source(csvRecord.toMap(), XContentType.JSON);

            // Perform the indexing and handle the response
            try {
                client.index(indexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                System.err.println("Failed to index record: " + csvRecord.toString());
                e.printStackTrace();
            }
        }
    } catch (IOException e) {
        System.err.println("Error reading CSV file: " + e.getMessage());
        e.printStackTrace();
    }
}
    public static void main(String[] args) {
        try {
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
            CSVToElasticsearch importer = new CSVToElasticsearch(client);
            importer.importCSV("G:\\bigdataPJ\\ProjectCode\\dataset_sdn.csv"); // Change this to your CSV file path
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
