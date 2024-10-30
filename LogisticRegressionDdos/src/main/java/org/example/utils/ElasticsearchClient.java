package org.example.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticsearchClient {
    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
    );

    public static RestHighLevelClient getClient() {
        return client;
    }

    public static void closeClient() throws IOException {
        client.close();
    }
}

