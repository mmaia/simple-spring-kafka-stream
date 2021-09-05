package com.resona.springkafkastream.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class StockQuoteProducer {

    private final KafkaTemplate<String, StockQuote> quoteProducer;
    private final static String TOPIC = "stock-quotes";

    public void send(StockQuote message) {
        quoteProducer.send(TOPIC, message.getSymbol().toString(), message);
    }
}
