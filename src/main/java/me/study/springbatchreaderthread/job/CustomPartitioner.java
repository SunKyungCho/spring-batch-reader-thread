package me.study.springbatchreaderthread.job;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

public class CustomPartitioner implements Partitioner {

    private final String table;
    private final String column;
    private final JdbcOperations jdbcTemplate;

    public CustomPartitioner(String table, String column, DataSource dataSource) {
        this.table = table;
        this.column = column;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }


    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long min = getQueryResult(RangeQueryType.MIN);
        long max = getQueryResult(RangeQueryType.MAX);
        long targetSize = (max - min) / gridSize + 1;

        Map<String, ExecutionContext> result = new HashMap<>();
        int number = 0;
        long start = min;
        long end = start + targetSize - 1;

        while (start <= max) {
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + number, value);

            if (end >= max) {
                end = max;
            }
            value.putLong("minValue", start);
            value.putLong("maxValue", end);
            start += targetSize;
            end += targetSize;
            number++;
        }

        return result;
    }

    private Long getQueryResult(RangeQueryType queryType) {
        Long result = jdbcTemplate.queryForObject(String.format("SELECT %s(%s) from %s", queryType.name(), column, table), Long.class);
        if (Objects.isNull(result)) {
            throw new IllegalStateException("");
        }
        return result;
    }

    private enum RangeQueryType {
        MIN, MAX
    }
}
