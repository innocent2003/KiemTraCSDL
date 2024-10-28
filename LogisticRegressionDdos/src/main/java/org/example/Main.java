package org.example;

import com.opencsv.CSVReader; // Importing OpenCSV's CSVReader
import java.io.FileReader; // Importing FileReader for file operations
import java.io.IOException; // Importing IOException for handling file errors

public class Main {
    public static void main(String[] args) {

        String csvFilePath = "G:\\bigdataPJ\\dataset_sdn.csv"; // Path to your CSV file

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] nextLine; // Array to hold each line of the CSV

            // Read each line in the CSV file
            while ((nextLine = reader.readNext()) != null) {
                // Print each column in the line
                for (String column : nextLine) {
                    System.out.print(column + "\t"); // Tab-separated for better readability
                }
                System.out.println(); // New line after each record
            }
        } catch (IOException e) {
            System.err.println("Error reading the CSV file: " + e.getMessage()); // Improved error handling
        } catch (Exception e) {
            e.printStackTrace(); // General error handling
        }
    }
}
