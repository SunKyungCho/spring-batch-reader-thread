package me.study.springbatchreaderthread.entity;

import java.time.LocalDateTime;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import org.hibernate.annotations.CreationTimestamp;

import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long productNo;
    private String name;
    private int price;
    private String brandName;
    private String imageUrl;
    private String partnerName;

    protected Product() {}

    public Product(String name, int price, String brandName, String imageUrl, String partnerName) {
        this.name = name;
        this.price = price;
        this.brandName = brandName;
        this.imageUrl = imageUrl;
        this.partnerName = partnerName;
    }

    @CreationTimestamp
    private LocalDateTime createAt;

}
