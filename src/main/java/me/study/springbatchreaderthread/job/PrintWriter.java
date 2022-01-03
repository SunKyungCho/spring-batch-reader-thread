package me.study.springbatchreaderthread.job;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import me.study.springbatchreaderthread.entity.Product;

@Slf4j
@Component
public class PrintWriter implements ItemWriter<Product> {


    @Override
    public void write(List<? extends Product> items) throws Exception {
        Thread.sleep(1000);
        List<Long> itemNumbers = items.stream().map(Product::getProductNo).collect(Collectors.toList());
        log.info("Load {} to {} >> size : {} no = {}", items.get(0).getProductNo(), items.get(items.size() - 1).getProductNo(), items.size(), itemNumbers);
    }
}
