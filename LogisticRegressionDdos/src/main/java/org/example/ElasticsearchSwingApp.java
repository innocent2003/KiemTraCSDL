package org.example;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchSwingApp extends JFrame {
    private static final String INDEX_NAME = "sdn"; // Thay bằng tên chỉ mục của bạn
    private static final Scroll SCROLL = new Scroll(TimeValue.timeValueMinutes(1L));
    private static RestHighLevelClient client;

    private JTable table;
    private DefaultTableModel tableModel;

    public ElasticsearchSwingApp() {
        setTitle("Elasticsearch Data Display");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        // Cấu hình JTable
        tableModel = new DefaultTableModel();
        table = new JTable(tableModel);
        JScrollPane scrollPane = new JScrollPane(table);
        add(scrollPane, BorderLayout.CENTER);

        // Thêm nút tải dữ liệu
        JButton loadButton = new JButton("Load Data");
        loadButton.addActionListener(e -> loadData());
        add(loadButton, BorderLayout.SOUTH);

        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    private void loadData() {
        List<SearchHit> allHits;
        try {
            allHits = fetchAllData();

            if (allHits.size() > 0) {
                // Xóa các cột hiện tại và thêm cột mới từ dữ liệu Elasticsearch
                tableModel.setRowCount(0);
                tableModel.setColumnCount(0);

                // Lấy cột từ JSON
                Map<String, Object> firstHit = allHits.get(0).getSourceAsMap();
                for (String column : firstHit.keySet()) {
                    tableModel.addColumn(column);
                }

                // Thêm dữ liệu vào bảng
                for (SearchHit hit : allHits) {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    Object[] rowData = sourceMap.values().toArray();
                    tableModel.addRow(rowData);
                }
            } else {
                JOptionPane.showMessageDialog(this, "No data found in Elasticsearch index.", "Info", JOptionPane.INFORMATION_MESSAGE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<SearchHit> fetchAllData() throws IOException {
        List<SearchHit> searchHits = new ArrayList<>();

        // Khởi tạo Scroll
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.scroll(SCROLL);
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .size(1000));

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] hits = searchResponse.getHits().getHits();

        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                searchHits.add(hit);
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(SCROLL);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            hits = searchResponse.getHits().getHits();
        }

        // Sử dụng ClearScrollRequest để xóa scroll
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        if (!clearScrollResponse.isSucceeded()) {
            System.err.println("Failed to clear scroll.");
        }

        return searchHits;
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            ElasticsearchSwingApp app = new ElasticsearchSwingApp();
            app.setVisible(true);
        });
    }
}
