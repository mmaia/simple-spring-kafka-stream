package com.resona.springkafkastream.api;

import com.resona.springkafkastream.model.LeveragePriceDTO;
import com.resona.springkafkastream.model.StockQuoteDTO;
import com.resona.springkafkastream.repository.LeveragePrice;
import com.resona.springkafkastream.repository.LeveragePriceProducer;
import com.resona.springkafkastream.repository.StockQuote;
import com.resona.springkafkastream.repository.StockQuoteProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@RequiredArgsConstructor
@Slf4j
@Controller
@RequestMapping("api")
public class QuotesController {

    private final StockQuoteProducer stockQuoteProducer;
    private final LeveragePriceProducer leveragePriceProducer;

    @PostMapping("/quotes")
    public ResponseEntity<StockQuoteDTO> newQuote(@RequestBody StockQuoteDTO stockQuoteDTO) {
        log.info("stockQuote: {}", stockQuoteDTO.toString());
        StockQuote stockQuote = StockQuote.newBuilder()
                .setSymbol(stockQuoteDTO.getSymbol())
                .setTradeValue(stockQuoteDTO.getTradeValue().doubleValue())
                .build();
        stockQuoteProducer.send(stockQuote);
        return ResponseEntity.ok(stockQuoteDTO);
    }

    @PostMapping("/leverage")
    public ResponseEntity<LeveragePriceDTO> newLeveragePrice(@RequestBody LeveragePriceDTO leveragePriceDTO) {
        log.info("stockQuote: {}", leveragePriceDTO.toString());
        LeveragePrice leveragePrice = LeveragePrice.newBuilder()
                .setSymbol(leveragePriceDTO.getSymbol())
                .setLeveragePrice(leveragePriceDTO.getLeveragePrice().doubleValue())
                .build();
        leveragePriceProducer.send(leveragePrice);
        return ResponseEntity.ok(leveragePriceDTO);
    }

}
