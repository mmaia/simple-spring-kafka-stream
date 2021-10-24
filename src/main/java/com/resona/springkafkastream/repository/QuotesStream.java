package com.resona.springkafkastream.repository;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.util.function.BiFunction;

@Repository
@RequiredArgsConstructor
public class QuotesStream {

    private final StreamsBuilderFactoryBean myKStreamsBuilder;

    SpecificAvroSerde<LeveragePrice> leveragePriceSerde = new SpecificAvroSerde<>();
    private ReadOnlyKeyValueStore<String, LeveragePrice> leveragePriceView;

    @PostConstruct
    public void init() {
        leveragePriceSerde.configure(KafkaConfiguration.SERDE_CONFIG, false);
    }


    BiFunction<String, StockQuote, KeyValue<String, ProcessedQuote>> quoteMapper = (symbol, stockQuote) -> {
        final ProcessedQuote processedQuote = ProcessedQuote.newBuilder()
                .setSymbol(symbol)
                .setTradeValue(stockQuote.getTradeValue())
                .build();
        return new KeyValue<>(symbol, processedQuote);
    };

    @Bean
    public KStream<String, ProcessedQuote> kStream(StreamsBuilder streamsBuilder) {
        // read stream from quotes-topic
        KStream<String, StockQuote> stream = streamsBuilder.stream(KafkaConfiguration.QUOTES_TOPIC);

        // build GKTable from leverage price
        final GlobalKTable<String, LeveragePrice> leverageBySymbolGKTable = streamsBuilder
                .globalTable(KafkaConfiguration.LEVERAGE_PRICE_TOPIC,
                        Materialized.<String, LeveragePrice, KeyValueStore<Bytes, byte[]>>as("leverage-by-symbol-table")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(leveragePriceSerde));

        leveragePriceView = myKStreamsBuilder.getKafkaStreams().store("leverage-by-symbol-table", QueryableStoreTypes.keyValueStore());

        // join and transform
        KStream<String, ProcessedQuote> inStream = stream
                .map(quoteMapper::apply)
                .leftJoin(leverageBySymbolGKTable,
                        (symbol, processedQuote) -> symbol,
                        (processedQuote, leveragePrice) -> {
                            if(leveragePrice == null) return processedQuote;
                            processedQuote.setLeveragePrice(leveragePrice.getLeveragePrice());
                            return processedQuote;
                        });

        // branches it and pushes to proper topics
        new KafkaStreamBrancher<String, ProcessedQuote>()
                .branch((symbolKey, processedQuote) -> symbolKey.equalsIgnoreCase("APPL"), ks -> ks.to("appl-stocks-topic"))
                .branch((symbolKey, processedQuote) -> symbolKey.equalsIgnoreCase("GOOGL"), ks -> ks.to("googl-stocks-topic"))
                .defaultBranch(ks -> ks.to("all-other-stocks-topic"))
                .onTopOf(inStream);

        return inStream;
    }

    public LeveragePrice getLeveragePrice(String key) {
        return leveragePriceView.get(key);
    }

}