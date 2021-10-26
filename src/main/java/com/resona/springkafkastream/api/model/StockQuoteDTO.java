package com.resona.springkafkastream.api.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class StockQuoteDTO {
    private String symbol;
    private BigDecimal tradeValue;
}
