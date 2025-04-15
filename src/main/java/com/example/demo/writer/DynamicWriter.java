package com.example.demo.writer;

import com.example.demo.entity.DynamicRecord;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class DynamicWriter implements ItemWriter<DynamicRecord> {

    @Override
    public void write(Chunk<? extends DynamicRecord> chunk) {
        chunk.getItems().forEach(record -> System.out.println("Writing record: " + record.getFields()));
    }
}
