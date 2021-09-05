package com.resona.springkafkastream.api;

import com.resona.springkafkastream.model.StockQuoteDTO;
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
public class StockQuoteController {

    private final StockQuoteProducer stockQuoteProducer;

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

}
