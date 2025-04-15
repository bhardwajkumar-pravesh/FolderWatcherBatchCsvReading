package com.example.demo.entity;

import lombok.Data;
import java.util.Map;

@Data
public class DynamicRecord {
    private Map<String, String> fields;
}
