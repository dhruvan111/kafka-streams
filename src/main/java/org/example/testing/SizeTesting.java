package org.example.testing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SizeTesting {
    private static final String FILE_NAME = "test.txt";

    public static void main(String[] args) {
        String testData = "Dhruvan\n";
        writeToFile(testData);

        File file = new File(FILE_NAME);
        System.out.println("Size of File: " + file.length());
        System.out.println("Size of Data: " + testData.getBytes(StandardCharsets.UTF_8).length);
    }

    private static void writeToFile(String content) {
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(FILE_NAME, true));
            writer.write(content);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
