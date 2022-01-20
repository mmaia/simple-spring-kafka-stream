package com.resona.springkafkastream.api;

import com.resona.springkafkastream.api.model.LeveragePriceDTO;
import com.resona.springkafkastream.api.model.QuotesPerWindowDTO;
import com.resona.springkafkastream.api.model.StockQuoteDTO;
import com.resona.springkafkastream.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
@Controller
@RequestMapping("api")
public class QuotesController {

    private final StockQuoteProducer stockQuoteProducer;
    private final LeveragePriceProducer leveragePriceProducer;
    private final QuotesStream quotesStream;

    @GetMapping("leveragePrice/{instrumentSymbol}")
    public ResponseEntity<LeveragePriceDTO> getLeveragePrice(@PathVariable String instrumentSymbol) {
        LeveragePrice leveragePrice = quotesStream.getLeveragePrice(instrumentSymbol);
        // if quote doesn't exist in our local store we return no content.
        if(leveragePrice == null) return ResponseEntity.noContent().build();
        LeveragePriceDTO result = new LeveragePriceDTO();
        result.setSymbol(leveragePrice.getSymbol().toString());
        result.setLeverage(BigDecimal.valueOf(leveragePrice.getLeveragePrice()));
        return ResponseEntity.ok(result);
    }

    @PostMapping("/quotes")
    public ResponseEntity<StockQuoteDTO> newQuote(@RequestBody StockQuoteDTO stockQuoteDTO) {
        log.info("stockQuote: {}", stockQuoteDTO.toString());
        StockQuote stockQuote = StockQuote.newBuilder()
                .setSymbol(stockQuoteDTO.getSymbol())
                .setTradeValue(stockQuoteDTO.getTradeValue().doubleValue())
                .build();
        if(stockQuoteDTO.getIsoDateTime() != null) {
            stockQuote.setTradeTime(stockQuoteDTO.getIsoDateTime().toEpochMilli());
        }
        stockQuoteProducer.send(stockQuote);
        return ResponseEntity.ok(stockQuoteDTO);
    }

    @GetMapping("/quotes/count/{symbol}")
    public ResponseEntity<QuotesPerWindowDTO> getQuotesPerWindow(@PathVariable String symbol,
                                                                 @RequestParam("pastMinutes") Integer pastMinutes) {

        Instant end = Instant.now();
        Instant start = end.minusSeconds(pastMinutes * 60);

        Optional<QuotesPerWindow> quotesPerWindow = quotesStream.allCountedQuotesForInterval(symbol, start.toEpochMilli(), end.toEpochMilli());

        if(quotesPerWindow.isPresent()) {
            QuotesPerWindow res = quotesPerWindow.get();
            return ResponseEntity.ok(
                    QuotesPerWindowDTO.builder()
                            .symbol(symbol)
                            .count(res.getCount())
                            .start(start)
                            .end(end)
                            .build()
            );
        }

        return ResponseEntity.noContent().build();
    }

    @PostMapping("/leverage")
    public ResponseEntity<LeveragePriceDTO> newLeveragePrice(@RequestBody LeveragePriceDTO leveragePriceDTO) {
        log.info("stockQuote: {}", leveragePriceDTO.toString());
        LeveragePrice leveragePrice = LeveragePrice.newBuilder()
                .setSymbol(leveragePriceDTO.getSymbol())
                .setLeveragePrice(leveragePriceDTO.getLeverage().doubleValue())
                .build();
        leveragePriceProducer.send(leveragePrice);
        return ResponseEntity.ok(leveragePriceDTO);
    }

}
