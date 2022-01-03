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
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import lombok.RequiredArgsConstructor;
import me.study.springbatchreaderthread.entity.Product;
import me.study.springbatchreaderthread.job.step.CustomJdbcPagingItemReader;
import me.study.springbatchreaderthread.job.step.CustomJdbcPagingItemReaderBuilder;

@Configuration
@RequiredArgsConstructor
public class MultiJdbcPagingReaderJob {

    private static final int CHUNK_SIZE = 500;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public Job multiThreadJdbcPagingReaderJob() {
        return jobBuilderFactory.get("multiThreadJdbcPagingReaderJob")
                                .start(multiThreadJdbcPagingReaderStep())
                                .build();
    }

    @Bean
    public Step multiThreadJdbcPagingReaderStep() {
        return stepBuilderFactory.get("multiThreadJdbcPagingReaderStep")
                                 .<Product, Product>chunk(CHUNK_SIZE)
                                 .reader(multiJdbcPagingReader())
                                 .writer(new PrintWriter())
                                 .taskExecutor(taskExecutor())
                                 .throttleLimit(10)
                                 .build();
    }

    private TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("async-");
        executor.initialize();
        return executor;
    }

    @Bean
    public CustomJdbcPagingItemReader<Product> multiJdbcPagingReader() {
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
