package me.study.springbatchreaderthread.test;

import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.transaction.Transactional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;

import me.study.springbatchreaderthread.entity.Product;

@SpringBootTest
class TestDataSetup {

    @Autowired
    ProductRepository productRepository;

    @Test
    @Transactional
    @Rollback(value = false)
    void test() {
        Random random = new Random();
        IntStream.rangeClosed(5001, 10000)
                 .mapToObj(x -> new Product(key(), random.nextInt(), key(), key(), key()))
                 .forEach(product -> productRepository.save(product));
    }

    private String key() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

}
