package org.example.views;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class LogisticPredictionView {
    public static void main(String[] args) {
        JFrame frame = new JFrame("Nhập Dữ Liệu");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(1000, 1500);
        frame.setLocationRelativeTo(null);  // Center the frame

        // Tạo thanh menu
        JMenuBar menuBar = new JMenuBar();

        // Tạo các phần trong menu
        JMenu dataMenu = new JMenu("Data");
        JMenu csvMenu = new JMenu("CSV");
        JMenu chartMenu = new JMenu("Chart");
        JMenu helpMenu = new JMenu("Help");

        // Thêm các phần vào thanh menu
        menuBar.add(dataMenu);
        menuBar.add(csvMenu);
        menuBar.add(chartMenu);
        menuBar.add(helpMenu);

        frame.setJMenuBar(menuBar);

        JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        panel.setBackground(new Color(245, 245, 245));
        frame.add(panel);

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(10, 10, 10, 10); // Padding between components
        gbc.fill = GridBagConstraints.HORIZONTAL;

        // Title
        JLabel titleLabel = new JLabel("Nhập Thông Tin Kết Nối");
        titleLabel.setFont(new Font("Arial", Font.BOLD, 18));
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2; // Span across two columns
        panel.add(titleLabel, gbc);

        // Input Fields Labels and Text Fields
        String[] labels = {
                "Switch:", "Pkt Count:", "Byte Count:", "Dur:", "Dur Nsec:", "Total Dur:",
                "Flows:", "Packet Ins:", "Pkt Per Flow:", "Byte Per Flow:", "Pkt Rate:",
                "Pair Flow:", "Protocol:", "Port No:", "Tx Bytes:", "Rx Bytes:",
                "Tx Kbps:", "Rx Kbps:"
        };

        JTextField[] textFields = new JTextField[labels.length];
        for (int i = 0; i < labels.length; i++) {
            gbc.gridwidth = 1; // Reset to 1 column
            gbc.gridx = 0;
            gbc.gridy = i + 1;
            JLabel label = new JLabel(labels[i]);
            label.setFont(new Font("Arial", Font.PLAIN, 14));
            panel.add(label, gbc);

            gbc.gridx = 1;
            textFields[i] = new JTextField(20);
            panel.add(textFields[i], gbc);
        }

        // Submit Button
        JButton submitButton = new JButton("Dự đoán");
        submitButton.setBackground(new Color(0, 123, 255));
        submitButton.setForeground(Color.WHITE);
        submitButton.setFont(new Font("Arial", Font.BOLD, 14));
        gbc.gridwidth = 2; // Span across two columns
        gbc.gridx = 0;
        gbc.gridy = labels.length + 1;
        panel.add(submitButton, gbc);

        // JLabel dưới nút Dự đoán
        JLabel resultLabel = new JLabel("Kết quả sẽ hiển thị ở đây");
        resultLabel.setFont(new Font("Arial", Font.PLAIN, 14));
        resultLabel.setForeground(Color.RED);
        resultLabel.setVisible(false); // Bắt đầu ở trạng thái ẩn
        gbc.gridy = labels.length + 2; // Đặt vị trí dưới nút Dự đoán
        panel.add(resultLabel, gbc);

        // Action Listener cho nút Dự đoán
        submitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Hiển thị kết quả
                resultLabel.setText("Dữ liệu đã được dự đoán!");
                resultLabel.setVisible(true); // Hiện label
            }
        });

        frame.setVisible(true);
    }
}
