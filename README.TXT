package com.example.batch.config;

import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.JobContext;
import org.springframework.batch.core.scope.StepScope;
import org.springframework.batch.core.scope.JobScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Configuration
@EnableBatchProcessing
public class CashExceptionsBatchJobConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private LoadReferenceDataTasklet loadReferenceDataTasklet;

    @Autowired
    private ProcessCashExceptionsTasklet cashExceptionsTasklet;

    @Autowired
    private CreateExceptionEntriesTasklet createExceptionEntriesTasklet;

    @Autowired
    private LoadRS2InvalidDataTasklet loadRS2InvalidDataTasklet;

    @Autowired
    private CashExceptionsBatchJobListener jobListener;

    @Autowired
    private ExecutionContextPromotionListener promotionListener;

    // Define Job
    @Bean
    public Job cashExceptionsProcessJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return jobBuilderFactory.get("cashExceptionsProcessJob")
                .start(loadRefDataStep(transactionManager))
                .next(cashExceptionsStep(transactionManager))
                .next(createExceptionEntriesStep(transactionManager))
                .next(loadRS2InvalidDataStep(transactionManager))
                .listener(jobListener)  // Attach the job listener
                .build();
    }

    // Step 1: Load Reference Data Step
    @Bean
    @JobScope
    public Step loadRefDataStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("loadRefDataStep")
                .tasklet(loadReferenceDataTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 2: Cash Exceptions Step
    @Bean
    @JobScope
    public Step cashExceptionsStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("cashExceptionsStep")
                .tasklet(cashExceptionsTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 3: Create Exception Entries Step
    @Bean
    @JobScope
    public Step createExceptionEntriesStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("createExceptionEntriesStep")
                .tasklet(createExceptionEntriesTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 4: Load RS2 Invalid Data Step
    @Bean
    @JobScope
    public Step loadRS2InvalidDataStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("loadRS2InvalidDataStep")
                .tasklet(loadRS2InvalidDataTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Define Promotion Listener
    @Bean
    @StepScope
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{"domainDataKey", "cashPayloadData", "exceptTranList"});
        return listener;
    }

    // Tasklet Definitions with Scope
    @Bean
    @StepScope
    public LoadReferenceDataTasklet loadReferenceDataTasklet(
            @Value("${batch.service.acct.plan.ref.query}") String acctPlanRefSql,
            @Value("${batch.service.holiday.ref.query}") String holidayRefSql,
            @Value("${batch.service.state.iso.ref.query}") String stateIsoRefSql,
            @Value("${batch.service.tran.code.ref.query}") String tranCodeRefSql) {
        LoadReferenceDataTasklet tasklet = new LoadReferenceDataTasklet();
        tasklet.setAcctPlanRefSql(acctPlanRefSql);
        tasklet.setHolidayRefSql(holidayRefSql);
        tasklet.setStateIsoRefSql(stateIsoRefSql);
        tasklet.setTranCodeRefSql(tranCodeRefSql);
        return tasklet;
    }

    @Bean
    @StepScope
    public ProcessCashExceptionsTasklet cashExceptionsTasklet(
            @Value("#{jobParameters['feed.recon.acct.ent.missing.query']}") String tranStatSql,
            @Value("#{jobParameters['feed.recon.acct.ent.stat.query']}") String tranStatInsertSql,
            @Value("#{jobParameters['feed.recon.acct.ent.impacs.query']}") String cashStatInsertSql,
            @Value("#{jobParameters['feed.recon.acct.plan.ref.query']}") String acctPlanInsertSql) {
        ProcessCashExceptionsTasklet tasklet = new ProcessCashExceptionsTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setTranStatSql(tranStatSql);
        tasklet.setTranStatInsertSql(tranStatInsertSql);
        tasklet.setCashStatInsertSql(cashStatInsertSql);
        tasklet.setAcctPlanInsertSql(acctPlanInsertSql);
        return tasklet;
    }

    @Bean
    @StepScope
    public CreateExceptionEntriesTasklet createExceptionEntriesTasklet(
            @Value("#{jobParameters['recon.except.ent.insert']}") String exceptEntInsert,
            @Value("#{jobParameters['recon.except.ent.rs2v.insert']}") String exceptEntRS2vInsert) {
        CreateExceptionEntriesTasklet tasklet = new CreateExceptionEntriesTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setExceptEntInsert(exceptEntInsert);
        tasklet.setExceptEntRS2vInsert(exceptEntRS2vInsert);
        return tasklet;
    }

    @Bean
    @StepScope
    public LoadRS2InvalidDataTasklet loadRS2InvalidDataTasklet(
            @Value("#{jobParameters['feed.mds.except.query']}") String feedMDSExceptQuery,
            @Value("#{jobParameters['feed.mds.except.invalid.query']}") String feedMDSExceptInvalidQuery) {
        LoadRS2InvalidDataTasklet tasklet = new LoadRS2InvalidDataTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setFeedMDSExceptQuery(feedMDSExceptQuery);
        tasklet.setFeedMDSExceptInvalidQuery(feedMDSExceptInvalidQuery);
        return tasklet;
    }

    // Job Listener Definition
    @Bean
    @JobScope
    public CashExceptionsBatchJobListener jobListener() {
        return new CashExceptionsBatchJobListener();
    }
}

// Tasklet Class Definitions

@Component
@StepScope
class LoadReferenceDataTasklet implements Tasklet {

    @Value("${batch.service.acct.plan.ref.query}")
    private String acctPlanRefSql;

    @Value("${batch.service.holiday.ref.query}")
    private String holidayRefSql;

    @Value("${batch.service.state.iso.ref.query}")
    private String stateIsoRefSql;

    @Value("${batch.service.tran.code.ref.query}")
    private String tranCodeRefSql;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Tasklet logic here
        return RepeatStatus.FINISHED;
    }

    public void setAcctPlanRefSql(String acctPlanRefSql) { this.acctPlanRefSql = acctPlanRefSql; }
    public void setHolidayRefSql(String holidayRefSql) { this.holidayRefSql = holidayRefSql; }
    public void setStateIsoRefSql(String stateIsoRefSql) { this.stateIsoRefSql = stateIsoRefSql; }
    public void setTranCodeRefSql(String tranCodeRefSql) { this.tranCodeRefSql = tranCodeRefSql; }
}

@Component
@StepScope
class ProcessCashExceptionsTasklet implements Tasklet {

    @Value("#{jobParameters['feed.recon.acct.ent.missing.query']}")
    private String tranStatSql;

    @Value("#{jobParameters['feed.recon.acct.ent.stat.query']}")
    private String tranStatInsertSql;

    @Value("#{jobParameters['feed.recon.acct.ent.impacs.query']}")
    private String cashStatInsertSql;

    @Value("#{jobParameters['feed.recon.acct.plan.ref.query']}")
    private String acctPlanInsertSql;

    @Autowired
    private DataSource dataSource;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Tasklet logic here
        return RepeatStatus.FINISHED;
    }

    public void setDataSource(DataSource dataSource) { this.dataSource = dataSource; }
    public void setTranStatSql(String tranStatSql) { this.tranStatSql = tranStatSql; }
    public void setTranStatInsertSql(String tranStatInsertSql) {
