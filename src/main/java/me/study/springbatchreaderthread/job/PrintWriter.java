package me.study.springbatchreaderthread.job;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import me.study.springbatchreaderthread.entity.Product;

@Slf4j
@Component
public class PrintWriter implements ItemWriter<Product> {


    @Override
    public void write(List<? extends Product> items) throws Exception {
        log.info("Load {} to {} >> size : {}", items.get(0).getProductNo(), items.get(items.size() - 1).getProductNo(), items.size());
    }
}
