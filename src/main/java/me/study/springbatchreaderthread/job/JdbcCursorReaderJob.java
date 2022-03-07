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
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import lombok.RequiredArgsConstructor;
import me.study.springbatchreaderthread.entity.Product;


@Configuration
@RequiredArgsConstructor
public class JdbcCursorReaderJob {

    private static final int CHUNK_SIZE = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public Job singleProcessJdbcCursorReaderJob() {
        return jobBuilderFactory.get("singleProcessJdbcCursorReaderJob")
                                .start(singleProcessJdbcCursorReaderStep())
                                .build();
    }

    @Bean
    public Step singleProcessJdbcCursorReaderStep() {
        return stepBuilderFactory.get("singleProcessJdbcCursorReaderStep")
                                 .<Product, Product>chunk(CHUNK_SIZE)
                                 .reader(jdbcCursorReader())
                                 .writer(new PrintWriter())
                                 .build();
    }

    @Bean
    public JdbcCursorItemReader<Product> jdbcCursorReader() {
        return new JdbcCursorItemReaderBuilder<Product>()
            .dataSource(dataSource)
            .fetchSize(CHUNK_SIZE)
            .rowMapper(new BeanPropertyRowMapper<>(Product.class))
            .sql("select * from product")
            .name("jdbcCursorItemReader")
            .build();
    }
}
