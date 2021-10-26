package com.resona.springkafkastream.repository;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
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

    public static final String LEVERAGE_BY_SYMBOL_TABLE = "leverage-by-symbol-ktable";
    SpecificAvroSerde<LeveragePrice> leveragePriceSerde = new SpecificAvroSerde<>();
    private ReadOnlyKeyValueStore<String, LeveragePrice> leveragePriceView;

    BiFunction<String, StockQuote, KeyValue<String, ProcessedQuote>> quoteMapper = (symbol, stockQuote) -> {
        final ProcessedQuote processedQuote = ProcessedQuote.newBuilder()
                .setSymbol(symbol)
                .setTradeValue(stockQuote.getTradeValue())
                .build();
        return new KeyValue<>(symbol, processedQuote);
    };

    @PostConstruct
    public void init() {
        leveragePriceSerde.configure(KafkaConfiguration.SERDE_CONFIG, false);
    }

    @Bean
    GlobalKTable<String, LeveragePrice> leveragePriceBySymbolGKTable(StreamsBuilder streamsBuilder) {
        // build GKTable from leverage price
        return streamsBuilder
                .globalTable(KafkaConfiguration.LEVERAGE_PRICE_TOPIC,
                        Materialized.<String, LeveragePrice, KeyValueStore<Bytes, byte[]>>as(LEVERAGE_BY_SYMBOL_TABLE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(leveragePriceSerde));
    }

    @Bean
    StreamsBuilderFactoryBean.Listener afterStart(StreamsBuilderFactoryBean sbfb) {
        StreamsBuilderFactoryBean.Listener listener = new StreamsBuilderFactoryBean.Listener() {
            @Override
            public void streamsAdded(String id, KafkaStreams streams) {
                leveragePriceView = streams.store(LEVERAGE_BY_SYMBOL_TABLE, QueryableStoreTypes.keyValueStore());
            }
        };
        sbfb.addListener(listener);
        return listener;
    }

    @Bean
    KStream<String, ProcessedQuote> kStream(StreamsBuilder streamsBuilder, GlobalKTable<String, LeveragePrice> leveragePriceBySymbolGKTable) {
        // read stream from quotes-topic
        KStream<String, StockQuote> stream = streamsBuilder.stream(KafkaConfiguration.QUOTES_TOPIC);

        // join and transform
        KStream<String, ProcessedQuote> inStream = stream
                .map(quoteMapper::apply)
                .leftJoin(leveragePriceBySymbolGKTable,
                        (symbol, processedQuote) -> symbol,
                        (processedQuote, leveragePrice) -> {
                            if(leveragePrice == null) return processedQuote;
                            processedQuote.setLeveragePrice(leveragePrice.getLeveragePrice());
                            return processedQuote;
                        });

        // branches it and pushes to proper topics
        new KafkaStreamBrancher<String, ProcessedQuote>()
                .branch((symbolKey, processedQuote) -> symbolKey.equalsIgnoreCase("APPL"), ks -> ks.to(KafkaConfiguration.APPL_STOCKS_TOPIC))
                .branch((symbolKey, processedQuote) -> symbolKey.equalsIgnoreCase("GOOGL"), ks -> ks.to(KafkaConfiguration.GOOGL_STOCKS_TOPIC))
                .defaultBranch(ks -> ks.to(KafkaConfiguration.ALL_OTHER_STOCKS_TOPIC))
                .onTopOf(inStream);

        return inStream;
    }

    public LeveragePrice getLeveragePrice(String key) {
        return leveragePriceView.get(key);
    }

}
