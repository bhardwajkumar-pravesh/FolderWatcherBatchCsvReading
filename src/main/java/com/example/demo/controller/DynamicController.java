package com.example.demo.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DynamicController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @PostMapping("/processFile")
    public String processFile(@RequestParam String filePath) throws Exception {
        System.out.println("Processing file: " + filePath);
        jobLauncher.run(job, new JobParametersBuilder()
                .addString("filePath", filePath)
                .toJobParameters());
        return "Processing started for " + filePath;
    }
}
