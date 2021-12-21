package me.study.springbatchreaderthread.test;

import org.springframework.data.repository.CrudRepository;

import me.study.springbatchreaderthread.entity.Product;

public interface ProductRepository extends CrudRepository<Product, Long> {

}
