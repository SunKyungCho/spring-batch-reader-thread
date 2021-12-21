package me.study.springbatchreaderthread.job;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import lombok.RequiredArgsConstructor;
import me.study.springbatchreaderthread.entity.Product;


@Configuration
@RequiredArgsConstructor
public class MultiJdbcCursorReaderJob {

    private static final int CHUNK_SIZE = 5000;
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
                                 .reader(reader())
                                 .writer(new PrintWriter())
                                 .taskExecutor(taskExecutor())
                                 .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(10);
        executor.setThreadNamePrefix("async-");
        executor.initialize();
        return executor;
    }

    @Bean
    public JdbcCursorItemReader<Product> reader() {
        return new JdbcCursorItemReaderBuilder<Product>()
            .dataSource(dataSource)
            .fetchSize(CHUNK_SIZE)
            .rowMapper(new BeanPropertyRowMapper<>(Product.class))
            .sql("select * from product")
            .name("jdbcCursorItemReader")
            .build();
    }
}
