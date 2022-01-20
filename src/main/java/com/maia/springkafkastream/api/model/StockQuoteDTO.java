package com.maia.springkafkastream.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
public class StockQuoteDTO {
    private String symbol;
    private BigDecimal tradeValue;
    @JsonFormat(shape = JsonFormat.Shape.STRING, timezone = "UTC")
    private Instant isoDateTime;
}
