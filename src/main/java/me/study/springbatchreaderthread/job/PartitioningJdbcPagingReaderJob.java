package me.study.springbatchreaderthread.job;


import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.Order;
import org.springframework.beans.factory.annotation.Value;
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
public class PartitioningJdbcPagingReaderJob {

    private static final int CHUNK_SIZE = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public Job partitioningThreadJdbcPagingReaderJob() {
        return jobBuilderFactory.get("partitioningThreadJdbcPagingReaderJob")
                                .start(partitioningJdbcPagingReaderStep())
                                .build();
    }

    @Bean
    public Step partitioningJdbcPagingReaderStep() {
        return stepBuilderFactory.get("partitioningJdbcPagingReaderStep")
                                 .partitioner("step", new CustomPartitioner("product", "product_no", dataSource))
                                 .step(jdbcPagingReaderStep())
                                 .gridSize(10)
                                 .taskExecutor(taskExecutor())
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
    public Step jdbcPagingReaderStep() {
        return stepBuilderFactory.get("jdbcPagingReaderStep")
                                 .<Product, Product>chunk(CHUNK_SIZE)
                                 .reader(partitioningJdbcPagingReader(null, null))
                                 .writer(new PrintWriter())
                                 .build();
    }

    @Bean
    @StepScope
    public CustomJdbcPagingItemReader<Product> partitioningJdbcPagingReader(
        @Value("#{stepExecutionContext['minValue']}") Long minValue,
        @Value("#{stepExecutionContext['maxValue']}") Long maxValue
    ) {
        return new CustomJdbcPagingItemReaderBuilder<Product>()
            .dataSource(dataSource)
            .selectClause("*")
            .whereClause(String.format("product_no between %s and %s", minValue, maxValue))
            .fromClause("product")
            .sortKeys(Map.of("product_no", Order.ASCENDING))
            .rowMapper(new BeanPropertyRowMapper<>(Product.class))
            .pageSize(CHUNK_SIZE)
            .saveState(false)
            .build();
    }
}
