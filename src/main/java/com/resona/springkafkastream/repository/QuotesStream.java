package com.resona.springkafkastream.repository;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.BiFunction;

@Repository
@RequiredArgsConstructor
public class QuotesStream {

    public static final String LEVERAGE_BY_SYMBOL_TABLE = "leverage-by-symbol-ktable";
    private final SpecificAvroSerde<LeveragePrice> leveragePriceSerde = new SpecificAvroSerde<>();
    private ReadOnlyKeyValueStore<String, LeveragePrice> leveragePriceView;

    public static final String QUOTES_PER_WINDOW_TABLE = "quotes-per-window-ktable";
    private final SpecificAvroSerde<QuotesPerWindow> quotesPerWindowSerde = new SpecificAvroSerde<>();
    private ReadOnlyWindowStore<String, Long> quotesPerWindowView;

    /**
     * From quote to processed quote
     */
    BiFunction<String, StockQuote, KeyValue<String, ProcessedQuote>> quoteMapper = (symbol, stockQuote) -> {
        final ProcessedQuote processedQuote = ProcessedQuote.newBuilder()
                .setSymbol(symbol)
                .setTradeValue(stockQuote.getTradeValue())
                .setTradeTime(stockQuote.getTradeTime())
                .build();
        return new KeyValue<>(symbol, processedQuote);
    };

    /**
     * From processed quote to quotes per window
     */
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
    StreamsBuilderFactoryBean.Listener afterStart(StreamsBuilderFactoryBean sbfb) {
        StreamsBuilderFactoryBean.Listener listener = new StreamsBuilderFactoryBean.Listener() {
            @Override
            public void streamsAdded(String id, KafkaStreams streams) {
                leveragePriceView = streams.store(StoreQueryParameters.fromNameAndType(LEVERAGE_BY_SYMBOL_TABLE, QueryableStoreTypes.keyValueStore()));
                quotesPerWindowView = streams.store(StoreQueryParameters.fromNameAndType(QUOTES_PER_WINDOW_TABLE, QueryableStoreTypes.windowStore()));
            }
        };
        sbfb.addListener(listener);
        return listener;
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
    KStream<String, ProcessedQuote> processedQuotesStream(StreamsBuilder streamsBuilder, GlobalKTable<String, LeveragePrice> leveragePriceBySymbolGKTable) {
        // read stream from quotes-topic
        KStream<String, StockQuote> stream = streamsBuilder.stream(KafkaConfiguration.QUOTES_TOPIC);

        // join and transform
        KStream<String, ProcessedQuote> inStream = stream
                .map(quoteMapper::apply)
                .leftJoin(leveragePriceBySymbolGKTable,
                        (symbol, processedQuote) -> symbol,
                        (processedQuote, leveragePrice) -> {
                            if (leveragePrice == null) return processedQuote;
                            processedQuote.setLeveragePrice(leveragePrice.getLeveragePrice());
                            return processedQuote;
                        });

        // branches it and pushes to proper topics
        new KafkaStreamBrancher<String, ProcessedQuote>()
                .branch((symbolKey, processedQuote) -> symbolKey.equalsIgnoreCase("APPL"), ks -> ks.to(KafkaConfiguration.APPL_STOCKS_TOPIC))
                .branch((symbolKey, processedQuote) -> symbolKey.equalsIgnoreCase("GOOGL"), ks -> ks.to(KafkaConfiguration.GOOGL_STOCKS_TOPIC))
                .defaultBranch(ks -> ks.to(KafkaConfiguration.ALL_OTHER_STOCKS_TOPIC))
                .onTopOf(inStream);



        // we can then group by symbol
        final KGroupedStream<String, ProcessedQuote> groupedBySymbol = inStream.groupByKey();

        // so we could then count total quotes by symbol
//        final KTable<String, Long> quotesCount = inStream.groupBy((symbolKey, processedQuote) -> symbolKey).count();
        // to read from the topic produced with the totals:
//         $ bin/kafka-console-consumer --topic count-total-by-symbol-topic --from-beginning \
//                              --bootstrap-server localhost:29092 \
//                               --property print.key=true \
//                               --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
        final KTable<String, Long> quotesCount = groupedBySymbol.count();
        // and send the values to a topic which could be a compacted topic from where we always get the latest total count per symbol.
        quotesCount.toStream().to(KafkaConfiguration.COUNT_TOTAL_QUOTES_BY_SYMBOL_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        // we can also group all time counts per stock per windowed interval
        final KTable<Windowed<String>, Long> quotesWindowed = groupedBySymbol.windowedBy(SessionWindows.with(Duration.ofSeconds(30))).count();
        // and then transform and send it to a new topic which we could then use a connector and send to elastic for example...
        quotesWindowed
                .toStream()
                .map(processedQuoteToQuotesPerWindowMapper::apply)
                .to(KafkaConfiguration.COUNT_WINDOW_QUOTES_BY_SYMBOL_TOPIC, Produced.with(Serdes.String(), quotesPerWindowSerde));

        // we could transform it materialize it to be able to query directly from kafka
        groupedBySymbol
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(QUOTES_PER_WINDOW_TABLE)
                        .withValueSerde(Serdes.Long()));

        return inStream;
    }

    public LeveragePrice getLeveragePrice(String key) {
        return leveragePriceView.get(key);
    }

    public Optional<QuotesPerWindow> allCountedQuotesForInterval(String key, Long start, Long end) {
        WindowStoreIterator<Long>  listOfCountedQuotes = quotesPerWindowView.fetch(key, Instant.ofEpochMilli(start), Instant.ofEpochMilli(end));

        if(!listOfCountedQuotes.hasNext()) return Optional.empty();

        long totalCountForInterval = 0;
        while(listOfCountedQuotes.hasNext()) {
           totalCountForInterval +=  listOfCountedQuotes.next().value;
        }

        var quotesPerWindow = QuotesPerWindow.newBuilder()
                .setSymbol(key)
                .setStartTime(start)
                .setEndTime(end)
                .setCount(totalCountForInterval)
                .build();

        return Optional.ofNullable(quotesPerWindow);
    }

}
