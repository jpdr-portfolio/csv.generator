package com.challenge.acc.csv.generator.service;

import com.challenge.acc.csv.generator.dto.SaleCsvDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.challenge.acc.csv.generator.util.Constants.CSV_SALE_ITEM_NAMES;

@Slf4j
@RequiredArgsConstructor
public class GeneratorService {

  private final Long csvRecordsCount;
  private final AtomicLong bytesCount = new AtomicLong(0);
  
  public void start(int cores) throws Exception {
    
    int availableCores = Math.max(1, Math.min(cores, Runtime.getRuntime().availableProcessors() - 1));
    long totalRecords = this.csvRecordsCount;
    long recordsPerThread = totalRecords / availableCores;
    long startMs = System.currentTimeMillis();
    
    log.info("Using " + availableCores + " threads to generate " + this.csvRecordsCount + " CSV records.");
    
    ExecutorService executor = Executors.newFixedThreadPool(availableCores);
    List<Future<Path>> futures = new ArrayList<>();
    for (int i = 0; i < availableCores; i++) {
      int threadId = i;
      long count = (i == availableCores - 1) ? totalRecords - recordsPerThread * i : recordsPerThread;
      
      futures.add(executor.submit(() -> generateTempFile(threadId, count)));
    }
    
    List<Path> tempFiles = new ArrayList<>();
    boolean errorOccurred = false;
    
    for (Future<Path> f : futures) {
      try{
        Path temp = f.get();
        tempFiles.add(f.get());
      }catch (ExecutionException ex){
        Throwable cause = ex.getCause();
        if (cause instanceof IOException) {
            System.err.println("Writing error: " + cause.getMessage());
            errorOccurred = true;
            break;
        } else {
            throw ex;
        }
      }
      
    }
    
    if (errorOccurred) {
      executor.shutdownNow();
      for (Future<?> f : futures) {
        f.cancel(true);
      }
      for (Path tempFile : tempFiles) {
        try {
          Files.deleteIfExists(tempFile);
        } catch (IOException e) {
          log.error("Unable to delete " + tempFile + ": " + e.getMessage());
        }
      }
      log.error("Process aborted due to insufficient disk space.");
      return;
    }
    
    executor.shutdown();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    String timestamp = LocalDateTime.now().format(formatter);
    Path finalFile = Paths.get("input_" + timestamp + ".csv");
    
    try (BufferedWriter writer = Files.newBufferedWriter(finalFile)) {
      for (Path tempFile : tempFiles) {
        Files.lines(tempFile).forEach(line -> {
          try {
            writer.write(line);
            writer.newLine();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
        Files.delete(tempFile);
      }
    }
    
    log.info("File has been created: " + finalFile.toAbsolutePath());
    log.info("A total of " + this.bytesCount.get() + " bytes where generated");
    log.info("It took a total time of " + (System.currentTimeMillis() - startMs) + " ms.");
    
    
  }
  
  public void start() throws Exception {
    
    int availableCores = 1;
    long startMs = System.currentTimeMillis();
    
    log.info("Using " + availableCores + " threads to generate " + this.csvRecordsCount + " CSV records.");
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    String timestamp = LocalDateTime.now().format(formatter);
    Path finalFile = Paths.get("input_" + timestamp + ".csv");
    
    generateFinalFile(this.csvRecordsCount, finalFile);
    
    log.info("File has been created: " + finalFile.toAbsolutePath());
    log.info("A total of " + this.bytesCount.get() + " bytes where generated");
    log.info("It took a total time of " + (System.currentTimeMillis() - startMs) + " ms.");
    
    
  }
  
  private void generateFinalFile(long recordsCount, Path finalFile) throws IOException{
    try (BufferedWriter writer = Files.newBufferedWriter(finalFile)) {
      String header = String.join(",", CSV_SALE_ITEM_NAMES);
      writer.write(header);
      writer.newLine();
      for (long i = 0; i < recordsCount; i++) {
          try {
            SaleCsvDto record = generateRecord();
            String line  = record.toString();
            this.bytesCount.addAndGet((line + System.lineSeparator()).length());
            writer.write(line);
            writer.newLine();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
      }
    }
  }
  
  
  
  private Path generateTempFile(int threadId, long count) throws IOException {
    Path tempFile = Files.createTempFile("input_" + threadId + "_", ".tmp");
    try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
      String header = String.join(",", CSV_SALE_ITEM_NAMES);
      writer.write(header);
      writer.newLine();
      for (long i = 0; i < count; i++) {
        SaleCsvDto record = generateRecord();
        String line  = record.toString();
        this.bytesCount.addAndGet((line + System.lineSeparator()).length());
        writer.write(line);
        writer.newLine();
      }
    }
    return tempFile;
  }
  
  private SaleCsvDto generateRecord(){
    Random random = new Random();
    int pointOfSale = random.nextInt(1,25);
    BigDecimal amount = BigDecimal.valueOf(random.nextDouble(100, 100001))
      .setScale(2, RoundingMode.UP);
    int quantity = random.nextInt(1,1000);
    int temperature = random.nextInt(-50, 50);
    int customerId = random.nextInt(1,Integer.MAX_VALUE);
    UUID productId = UUID.randomUUID();
    return SaleCsvDto.builder()
      .pointOfSale(pointOfSale)
      .amount(amount)
      .quantity(quantity)
      .temperature(temperature)
      .customerId(customerId)
      .productId(productId)
      .build();
  }
  

}
