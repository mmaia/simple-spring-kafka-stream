package com.resona.springkafkastream.repository;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.function.BiFunction;

@Repository
@RequiredArgsConstructor
public class QuotesStream {

    public static final String LEVERAGE_BY_SYMBOL_TABLE = "leverage-by-symbol-ktable";
    SpecificAvroSerde<LeveragePrice> leveragePriceSerde = new SpecificAvroSerde<>();
    private ReadOnlyKeyValueStore<String, LeveragePrice> leveragePriceView;
    SpecificAvroSerde<QuotesPerWindow> quotesPerWindowSerde = new SpecificAvroSerde<>();

    // From quote to processed quote
    BiFunction<String, StockQuote, KeyValue<String, ProcessedQuote>> quoteMapper = (symbol, stockQuote) -> {
        final ProcessedQuote processedQuote = ProcessedQuote.newBuilder()
                .setSymbol(symbol)
                .setTradeValue(stockQuote.getTradeValue())
                .build();
        return new KeyValue<>(symbol, processedQuote);
    };

    // From processed quote to quotes per window
    BiFunction<Windowed<String>, Long, KeyValue<String, QuotesPerWindow>> processedQuoteToQuotesPerWindowMapper = (key, value) -> {
        final QuotesPerWindow quotesPerWindow = QuotesPerWindow.newBuilder()
                .setSymbol(key.key())
                .setStartTime(key.window().start())
                .setEndTime(key.window().end())
                .setCount(value != null ? value : 0)
                .build();
        return new KeyValue<>(key.key(), quotesPerWindow);
    };

    @PostConstruct
    public void init() {
        leveragePriceSerde.configure(KafkaConfiguration.SERDE_CONFIG, false);
        quotesPerWindowSerde.configure(KafkaConfiguration.SERDE_CONFIG, false);
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


        // count quotes by symbol
//        final KTable<String, Long> quotesCount = inStream.groupBy((symbolKey, processedQuote) -> symbolKey).count();
        // to read from the topic produced with the totals:
//         $ bin/kafka-console-consumer --topic count-total-by-symbol-topic --from-beginning \
//                              --bootstrap-server localhost:29092 \
//                               --property print.key=true \
//                               --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
        final KTable<String, Long> quotesCount = inStream.groupByKey().count();
        quotesCount.toStream().to(KafkaConfiguration.COUNT_TOTAL_QUOTES_BY_SYMBOL_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // group counts per stock per interval
        final KTable<Windowed<String>, Long> quotesWindowed = inStream.groupByKey().windowedBy(SessionWindows.with(Duration.ofSeconds(30))).count();
        quotesWindowed
                .toStream()
                .map(processedQuoteToQuotesPerWindowMapper::apply)
                .to(KafkaConfiguration.COUNT_WINDOW_QUOTES_BY_SYMBOL_TOPIC, Produced.with(Serdes.String(), quotesPerWindowSerde));

        return inStream;
    }

    public LeveragePrice getLeveragePrice(String key) {
        return leveragePriceView.get(key);
    }

}
