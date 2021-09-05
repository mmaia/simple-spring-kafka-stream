package com.resona.springkafkastream.repository;


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.stereotype.Repository;

@Repository
public class StockQuoteStream {

    @Bean
    public KStream<String, StockQuote> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, StockQuote> stream = streamsBuilder.stream("stock-quotes");
        new KafkaStreamBrancher<String, StockQuote>()
                .branch((symbolKey, stockQuote) -> symbolKey.equalsIgnoreCase("APPL"), ks -> ks.to("appl-stocks-topic"))
                .branch((symbolKey, stockQuote) -> symbolKey.equalsIgnoreCase("GOOGL"), ks -> ks.to("googl-stocks-topic"))
                .defaultBranch(ks -> ks.to("all-other-stocks-topic"))
                .onTopOf(stream);
        return stream;
    }
}
