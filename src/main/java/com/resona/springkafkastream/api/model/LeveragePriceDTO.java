package com.resona.springkafkastream.api.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class LeveragePriceDTO {
    private String symbol;
    private BigDecimal leverage;
}
