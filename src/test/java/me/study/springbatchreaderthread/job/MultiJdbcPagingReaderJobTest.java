package me.study.springbatchreaderthread.job;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBatchTest
@SpringBootTest(classes = {MultiJdbcPagingReaderJob.class})
@EnableAutoConfiguration
@EnableBatchProcessing
class MultiJdbcPagingReaderJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void multi_thread_jdbc_paging_reader_test() throws Exception {

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        ExitStatus exitStatus = jobExecution.getExitStatus();

        long time = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();
        System.out.println(">>>>>>>> " + time + "ms");

        assertThat(exitStatus).isEqualTo(ExitStatus.COMPLETED);
    }
}