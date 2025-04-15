package com.example.demo.service;


import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
public class FolderWatcherService {

    @Autowired
    private final JobLauncher jobLauncher;

    @Autowired
    private final Job job;

    @Value("${app.watch.folder}")
    private String watchFolder;

    private static final Set<String> TARGET_FILES = Set.of("abc.csv", "xyz.csv", "pqr.csv");

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @PostConstruct
    public void startWatching() {
        executor.submit(this::watchFolder);
    }

    private void watchFolder() {
        try {
            Path folderPath = Paths.get(watchFolder);
            WatchService watchService = FileSystems.getDefault().newWatchService();
            folderPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            System.out.println("Watching folder: " + folderPath.toAbsolutePath());

            while (true) {
                WatchKey key = watchService.take(); // blocking
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    Path filename = (Path) event.context();
                    if (kind == StandardWatchEventKinds.ENTRY_CREATE &&
                            TARGET_FILES.contains(filename.toString())) {
                        try{
                        String fullPath = folderPath.resolve(filename).toAbsolutePath().toString();
                        System.out.println("Detected file: " + fullPath);

                        jobLauncher.run(job, new JobParametersBuilder()
                                .addString("filePath", fullPath)
                                .addLong("time", System.currentTimeMillis()) // unique
                                .toJobParameters());
                        } catch (Exception e) {
                            System.err.println("Failed to process file " + filename + ": " + e.getMessage());
                            e.printStackTrace();
                        }
                    }//end if
                }
                key.reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
