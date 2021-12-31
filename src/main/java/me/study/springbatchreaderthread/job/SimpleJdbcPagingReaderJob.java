package me.study.springbatchreaderthread.job;


import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import lombok.RequiredArgsConstructor;
import me.study.springbatchreaderthread.entity.Product;
import me.study.springbatchreaderthread.job.step.CustomJdbcPagingItemReader;
import me.study.springbatchreaderthread.job.step.CustomJdbcPagingItemReaderBuilder;

@Configuration
@RequiredArgsConstructor
public class SimpleJdbcPagingReaderJob {

    private static final int CHUNK_SIZE = 10;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public Job singleProcessJdbcPagingReaderJob() {
        return jobBuilderFactory.get("singleProcessJdbcPagingReaderJob")
                                .start(singleProcessJdbcPagingReaderStep())
                                .build();
    }

    @Bean
    public Step singleProcessJdbcPagingReaderStep() {
        return stepBuilderFactory.get("singleProcessJdbcPagingReaderStep")
                                 .<Product, Product>chunk(CHUNK_SIZE)
                                 .reader(simpleJdbcPagingReader())
                                 .writer(new PrintWriter())
                                 .build();
    }

//    @Bean
//    public CustomJdbcPagingItemReader<Product> reader() {
//        return new CustomJdbcPagingItemReaderBuilder<Product>()
//            .dataSource(dataSource)
//            .selectClause("*")
//            .fromClause("product")
//            .sortKeys(Map.of("product_no", Order.ASCENDING))
//            .rowMapper(new BeanPropertyRowMapper<>(Product.class))
//            .pageSize(CHUNK_SIZE)
//            .saveState(false)
//            .build();
//    }

    @Bean
    public CustomJdbcPagingItemReader<Product> simpleJdbcPagingReader() {
        return new CustomJdbcPagingItemReaderBuilder<Product>()
            .dataSource(dataSource)
            .selectClause("*")
            .fromClause("product")
            .sortKeys(Map.of("product_no", Order.ASCENDING))
            .rowMapper(new BeanPropertyRowMapper<>(Product.class))
            .pageSize(CHUNK_SIZE)
            .saveState(false)
            .build();
    }
}
