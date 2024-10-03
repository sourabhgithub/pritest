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
public class TchReconBatchJobConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private LoadReferenceDataTasklet loadReferenceDataTasklet;

    @Autowired
    private ProcessRejectedTransactionsTasklet rejectedTransactionsTasklet;

    @Autowired
    private ProcessUnmatchedStatusTransTasklet unmatchedStatusTransTasklet;

    @Autowired
    private TchReconProcessor tchReconProcessor;

    @Autowired
    private EndOfJobTasklet endOfJobTasklet;

    @Autowired
    private ExecutionContextPromotionListener promotionListener;

    @Autowired
    private TchReconBatchJobListener jobListener;

    // Define Job
    @Bean
    public Job tchReconProcessJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return jobBuilderFactory.get("tchReconProcessJob")
                .start(loadRefDataStep(transactionManager))
                .next(processReversalStep(transactionManager))
                .next(processUnmatchStatusStep(transactionManager))
                .next(reconProcessStep(transactionManager))
                .next(endOfJobProcessStep(transactionManager))
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

    // Step 2: Process Rejected Transactions Step
    @Bean
    @JobScope
    public Step processReversalStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("processReversalStep")
                .tasklet(rejectedTransactionsTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 3: Process Unmatched Status Step
    @Bean
    @JobScope
    public Step processUnmatchStatusStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("processUnmatchStatusStep")
                .tasklet(unmatchedStatusTransTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 4: Reconciliation Process Step
    @Bean
    @JobScope
    public Step reconProcessStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("reconProcessStep")
                .<TchReconViewObject, TchReconViewObject>chunk(100)
                .reader(viewItemReader(null))
                .processor(tchReconProcessor)
                .writer(compositeItemWriter())
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 5: End of Job Process Step
    @Bean
    @JobScope
    public Step endOfJobProcessStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("endOfJobProcessStep")
                .tasklet(endOfJobTasklet)
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
    public ProcessRejectedTransactionsTasklet rejectedTransactionsTasklet(
            @Value("${tch.recon.reject.acct.missing.query}") String missingQuery,
            @Value("${tch.recon.reject.acct.insert}") String insertSql) {
        ProcessRejectedTransactionsTasklet tasklet = new ProcessRejectedTransactionsTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setMissingQuery(missingQuery);
        tasklet.setInsertSql(insertSql);
        return tasklet;
    }

    @Bean
    @StepScope
    public ProcessUnmatchedStatusTransTasklet unmatchedStatusTransTasklet(
            @Value("${tch.recon.unmatched.status.query}") String unmatchedStatusQuery,
            @Value("${tch.recon.unmatched.status.insert}") String unmatchedStatusInsertSql) {
        ProcessUnmatchedStatusTransTasklet tasklet = new ProcessUnmatchedStatusTransTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setUnmatchedStatusQuery(unmatchedStatusQuery);
        tasklet.setUnmatchedStatusInsertSql(unmatchedStatusInsertSql);
        return tasklet;
    }

    @Bean
    @StepScope
    public EndOfJobTasklet endOfJobTasklet(
            @Value("${tch.recon.except.ent.rs2v.insert}") String exceptEntRS2vInsert) {
        EndOfJobTasklet tasklet = new EndOfJobTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setExceptEntRS2vInsert(exceptEntRS2vInsert);
        return tasklet;
    }

    // Define the Reader, Processor, and Writer beans
    @Bean
    @StepScope
    public JdbcCursorItemReader<TchReconViewObject> viewItemReader(@Value("${tch.recon.view.query}") String sql) {
        JdbcCursorItemReader<TchReconViewObject> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql(sql);
        reader.setRowMapper(new TchReconRowMapper());  // Assuming TchReconRowMapper exists
        return reader;
    }

    @Bean
    @StepScope
    public CompositeItemWriter<TchReconViewObject> compositeItemWriter() {
        CompositeItemWriter<TchReconViewObject> writer = new CompositeItemWriter<>();
        writer.setDelegates(Arrays.asList(acctEntryImpacsExceptItemWriter(), cashTranExceptItemWriter()));
        return writer;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<TchReconViewObject> acctEntryImpacsExceptItemWriter() {
        JdbcBatchItemWriter<TchReconViewObject> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("${tch.recon.acct.ent.impacs.except.insert}");
        return writer;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<TchReconViewObject> cashTranExceptItemWriter() {
        JdbcBatchItemWriter<TchReconViewObject> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("${tch.recon.cash.tran.except.insert}");
        return writer;
    }

    // Job Listener Definition
    @Bean
    @JobScope
    public TchReconBatchJobListener jobListener() {
        return new TchReconBatchJobListener();
    }
}
