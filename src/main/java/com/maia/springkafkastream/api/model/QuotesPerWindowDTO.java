package com.maia.springkafkastream.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class QuotesPerWindowDTO {
    private String symbol;
    private long count;
    @JsonFormat(shape = JsonFormat.Shape.STRING, timezone = "UTC")
    private Instant start;
    @JsonFormat(shape = JsonFormat.Shape.STRING, timezone = "UTC")
    private Instant end;
}
