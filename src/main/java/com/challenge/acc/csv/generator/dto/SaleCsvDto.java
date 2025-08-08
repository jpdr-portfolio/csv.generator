package com.challenge.acc.csv.generator.dto;

import lombok.Builder;

import java.math.BigDecimal;
import java.util.UUID;

@Builder
public record SaleCsvDto(
  Integer pointOfSale, BigDecimal amount, Integer quantity, Integer temperature,
    Integer customerId, UUID productId) {
  
  @Override
  public String toString(){
    return (pointOfSale) + "," +
        (amount) + "," +
          (quantity) + "," +
            (temperature) + "," +
              (customerId) + "," +
                (productId);
  }
}
