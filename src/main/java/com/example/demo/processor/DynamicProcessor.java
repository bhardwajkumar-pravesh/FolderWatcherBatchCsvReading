package com.example.demo.processor;

import com.example.demo.entity.DynamicRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class DynamicProcessor implements ItemProcessor<DynamicRecord, DynamicRecord> {
    @Override
    public DynamicRecord process(DynamicRecord record) {
        System.out.println("Processing record: " + record.getFields());
        return record;
    }
}
