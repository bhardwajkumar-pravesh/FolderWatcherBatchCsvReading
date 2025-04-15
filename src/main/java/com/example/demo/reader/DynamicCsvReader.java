package com.example.demo.reader;

import com.example.demo.entity.DynamicRecord;
import com.opencsv.CSVReader;
import jakarta.annotation.PostConstruct;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
@Component
@StepScope
public class DynamicCsvReader implements ItemReader<DynamicRecord> {

    private Iterator<String[]> dataIterator;
    private List<String> headers;

    @Value("#{jobParameters['filePath']}")
    private String filePath;

    @PostConstruct
    public void init() {
        loadCsvData();
    }

    private void loadCsvData() {
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath)))) {
            System.out.println("Opened file: " + filePath);
            List<String[]> records = reader.readAll();
            if (!records.isEmpty()) {
                headers = Arrays.asList(records.get(0));
                System.out.println("Headers found: " + headers);
                dataIterator = records.subList(1, records.size()).iterator();


            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading CSV file: " + e.getMessage(), e);
        }
    }

    @Override
    public DynamicRecord read() {
        if (dataIterator != null && dataIterator.hasNext()) {
            String[] dataRow = dataIterator.next();
            System.out.println("Reading row: " + Arrays.toString(dataRow));

            Map<String, String> dataMap = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                dataMap.put(headers.get(i), i < dataRow.length ? dataRow[i] : "");
            }
            DynamicRecord record = new DynamicRecord();
            record.setFields(dataMap);
            return record;
        }
        return null;
    }
}
