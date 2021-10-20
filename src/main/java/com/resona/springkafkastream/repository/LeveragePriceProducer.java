package com.resona.springkafkastream.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class LeveragePriceProducer {

    private final KafkaTemplate<String, LeveragePrice> leveragePriceProducer;

    public void send(LeveragePrice message) {
        leveragePriceProducer.send(KafkaConfiguration.LEVERAGE_PRICE_TOPIC, message.getSymbol().toString(), message);
    }
}
