package com.challenge.acc.csv.generator;


import com.challenge.acc.csv.generator.service.GeneratorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
  
  private static final String VERSION = "v1.0.0";
  
  public static void main(String[] args) throws Exception{
    log.info("CSV generator " + VERSION + ".");
    if (args == null || args.length == 0) {
      log.error("Error: Missing input parameters. Closing...");
      System.exit(0);
    }
    long csvRecordsCount = 0;
    try{
      csvRecordsCount = Long.parseLong(args[0]);
      GeneratorService generatorService = new GeneratorService(csvRecordsCount);
      generatorService.start();
    }catch (NumberFormatException ex){
      log.error("Error: Invalid CSV records count parameter. Closing...");
      System.exit(0);
    }
  }
  
}
