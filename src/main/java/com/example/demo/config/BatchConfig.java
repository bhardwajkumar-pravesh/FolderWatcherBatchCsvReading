package com.example.demo.config;

import com.example.demo.entity.DynamicRecord;
import com.example.demo.processor.DynamicProcessor;
import com.example.demo.reader.DynamicCsvReader;
import com.example.demo.writer.DynamicWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final int CHUNK_SIZE=1000;
    @Bean
    public Job job(JobRepository jobRepository, Step step1) {
        return new JobBuilder("dynamicCsvJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      DynamicCsvReader reader, // Using DynamicCsvReader directly here
                      DynamicProcessor processor,
                      DynamicWriter writer) {
        return new StepBuilder("csvStep", jobRepository)
                .<DynamicRecord, DynamicRecord>chunk(CHUNK_SIZE, transactionManager)
                .reader(reader) // Reader injected here
                .processor(processor)
                .writer(writer)
                .build();
    }

    // Removed the anonymous ItemReader bean and used the DynamicCsvReader
}
