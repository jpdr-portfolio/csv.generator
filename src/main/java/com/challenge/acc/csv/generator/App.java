package com.challenge.acc.csv.generator;

import com.challenge.acc.csv.generator.dto.SaleCsvDto;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

import static com.challenge.acc.csv.generator.util.Constants.CSV_SALE_ITEM_NAMES;

@Slf4j
public class App {

  private static final String VERSION = "v1.0.0";
  
  private final Long csvRecordsCount;
  private final String fileName;
  private final Random random = new Random();
  private long bytesCount = 0;
  
  public App(Long csvRecordsCount){
    this(csvRecordsCount, getRandomFileName());
  }
  
  public App(Long csvRecordsCount, String fileName){
    this.csvRecordsCount = csvRecordsCount;
    this.fileName = fileName;
  }
  
  public static void main(String[] args) throws Exception{
    log.info("CSV generator " + VERSION + ".");
    if (args == null || args.length == 0) {
      log.error("Error: Missing input parameters. Closing...");
      System.exit(0);
    }
    try{
      long csvRecordsCount = Long.parseLong(args[0]);
      App app = args.length > 1 ? new App(csvRecordsCount, args[1]) : new App(csvRecordsCount);
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
    log.info("The file will be stored as " + this.fileName);
    Path finalFile = Paths.get(this.fileName);
    Path tempFile = Files.createTempFile("", ".tmp");
    log.info("Temporal file " + tempFile.toString());
    log.info("Generating file...");
    generateFinalFiles(this.csvRecordsCount, tempFile);
    log.info("Moving file to final path");
    Files.move(tempFile, finalFile, StandardCopyOption.REPLACE_EXISTING);
    
    log.info("File has been created: " + finalFile.toAbsolutePath());
    log.info("A total of " + this.bytesCount + " bytes where generated");
    log.info("It took a total time of " + (System.currentTimeMillis() - startMs) + " ms.");
    
    
  }
  
  private static String getRandomFileName(){
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    String timestamp = LocalDateTime.now().format(formatter);
    return "/csv/input_" + timestamp + ".csv";
  }
  
  private void generateFinalFiles(long recordsCount, Path finalFile) throws IOException{
    
    long totalRows = 0;
    BigDecimal totalAmount = new BigDecimal(0);
    long totalQuantity = 0;
    
    try (BufferedWriter writer = Files.newBufferedWriter(finalFile)) {
      String header = String.join(",", CSV_SALE_ITEM_NAMES);
      writer.write(header);
      
      for (long i = 0; i < recordsCount; i++) {
        SaleCsvDto saleCsvDto = generateRecord(i + 1);
        String line = saleCsvDto.toString();
        writer.newLine();
        writer.write(line);
        this.bytesCount += (line + System.lineSeparator()).length();
        totalRows += 1;
        totalQuantity += saleCsvDto.quantity();
        totalAmount = totalAmount.add(saleCsvDto.amount());
        
      }
    }
    generateControlFile(totalRows, totalAmount, totalQuantity);
  }
  
  private void generateControlFile(Long totalRows, BigDecimal totalAmount, long totalQuantity)
    throws IOException{
    Path controlFile = Paths.get(this.fileName + ".control");
    try (BufferedWriter writer = Files.newBufferedWriter(controlFile)) {
      String line = totalRows + "|" + totalAmount + "|" + totalQuantity;
      writer.write(line);
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
