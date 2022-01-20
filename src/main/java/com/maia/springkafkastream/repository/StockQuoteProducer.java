package com.maia.springkafkastream.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class StockQuoteProducer {

    private final KafkaTemplate<String, StockQuote> quoteProducer;

    public void send(StockQuote message) {
        quoteProducer.send(KafkaConfiguration.QUOTES_TOPIC, message.getSymbol().toString(), message);
    }
}
