package com.challenge.acc.csv.generator;

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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.challenge.acc.csv.generator.util.Constants.CSV_SALE_ITEM_NAMES;

@Slf4j
@RequiredArgsConstructor
public class App {

  private static final String VERSION = "v1.0.0";
  
  private final Long csvRecordsCount;
  private final AtomicLong bytesCount = new AtomicLong(0);
  private final Random random = new Random();
  
  public static void main(String[] args) throws Exception{
    log.info("CSV generator " + VERSION + ".");
    if (args == null || args.length == 0) {
      log.error("Error: Missing input parameters. Closing...");
      System.exit(0);
    }
    try{
      long csvRecordsCount = Long.parseLong(args[0]);
      App app = new App(csvRecordsCount);
      app.start();
    }catch (NumberFormatException ex){
      log.error("Error: Invalid CSV records count parameter. Closing...");
      System.exit(0);
    }
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
            SaleCsvDto saleCsvDto = generateRecord(i+1);
            String line  = saleCsvDto.toString();
            this.bytesCount.addAndGet((line + System.lineSeparator()).length());
            writer.write(line);
            writer.newLine();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
      }
    }
  }
  
  private SaleCsvDto generateRecord(Long id){
    int pointOfSale = this.random.nextInt(1,25);
    BigDecimal amount = BigDecimal.valueOf(this.random.nextDouble(100, 100001))
      .setScale(2, RoundingMode.UP);
    int quantity = this.random.nextInt(1,1000);
    int temperature = this.random.nextInt(-50, 50);
    int customerId = this.random.nextInt(1,Integer.MAX_VALUE);
    UUID productId = UUID.randomUUID();
    return SaleCsvDto.builder()
      .id(id)
      .pointOfSale(pointOfSale)
      .amount(amount)
      .quantity(quantity)
      .temperature(temperature)
      .customerId(customerId)
      .productId(productId)
      .build();
  }
  

}
