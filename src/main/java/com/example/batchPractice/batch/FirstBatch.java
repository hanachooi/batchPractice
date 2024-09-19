package com.example.batchPractice.batch;

import com.example.batchPractice.entity.AfterEntity;
import com.example.batchPractice.entity.BeforeEntity;
import com.example.batchPractice.repository.AfterRepository;
import com.example.batchPractice.repository.BeforeRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;


@Configuration
public class FirstBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final BeforeRepository beforeRepository;
    private final AfterRepository afterRepository;

    public FirstBatch(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, BeforeRepository beforeRepository, AfterRepository afterRepository) {

        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.beforeRepository = beforeRepository;
        this.afterRepository = afterRepository;
    }

    // Job 클래스임. Job 은 하나의 배치를 실행시키는 역할을 함.

    @Bean
    public Job firstJob() {

        System.out.println("first job");

        // 아래는 작업 진행 여부를 메타데이터 테이블에 기록함
        // jobRepository 는 트랙킹할 repository 이므로, job 과 step 에서 동일하게 JobBuilder를 해주어야 한다.
        // job 하나에는 여러개의 step 이 정의가 되게 됌.
        return new JobBuilder("firstJob", jobRepository)
                .start(firstStep())
                // . next()
                .build();
    }

    // job 에서 실행되는 스탭들을 정의함
    @Bean
    public Step firstStep() {

        System.out.println("first step");

        // read -> process -> writer 과정으로 이루어 지는데, 이는 아래에서 메소드를 정의해주어야 한다.
        return new StepBuilder("firstStep", jobRepository)
                .<BeforeEntity, AfterEntity> chunk(10, platformTransactionManager)
                .reader(beforeReader())
                .processor(middleProcessor())
                .writer(afterWriter())
                .build();
    }

    // BeforeEntity 에서 데이터를 읽어오는 read 메서드 정의
    // 데이터가 꼬이면 안되기 때문에 id 를 기준으로 오름차순으로 데이터를 읽어옴.
    @Bean
    public RepositoryItemReader<BeforeEntity> beforeReader() {

        return new RepositoryItemReaderBuilder<BeforeEntity>()
                .name("beforeReader")
                .pageSize(10)
                .methodName("findAll")
                .repository(beforeRepository)
                .sorts(Map.of("id", Sort.Direction.ASC))
                .build();
    }

    // 읽어온 데이터를 처리하는 부분 ( process 메서드 정의  )
    @Bean
    public ItemProcessor<BeforeEntity, AfterEntity> middleProcessor() {

        return new ItemProcessor<BeforeEntity, AfterEntity>() {

            @Override
            public AfterEntity process(BeforeEntity item) throws Exception {

                AfterEntity afterEntity = new AfterEntity();
                afterEntity.setUsername(item.getUsername());

                return afterEntity;
            }
        };
    }

    // AfterEntity에 처리한 결과를 저장하는 writer ( 쓰기 메서드 정의 )
    @Bean
    public RepositoryItemWriter<AfterEntity> afterWriter() {

        return new RepositoryItemWriterBuilder<AfterEntity>()
                .repository(afterRepository)
                .methodName("save")
                .build();
    }
}