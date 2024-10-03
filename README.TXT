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
public class ZelleVisaReconJobBatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private LoadReferenceDataTasklet loadReferenceDataTasklet;

    @Autowired
    private ProcessReconExceptionsTasklet zelleVisaReconExceptionsTasklet;

    @Autowired
    private CreateReconExceptionsTasklet createZelleVisaReconExceptionsTasklet;

    @Autowired
    private CashExceptionsBatchJobListener jobListener;

    @Autowired
    private ExecutionContextPromotionListener promotionListener;

    // Define Job
    @Bean
    public Job cashExceptionsProcessJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return jobBuilderFactory.get("cashExceptionsProcessJob")
                .start(zelleVisaReconExceptionsStep(transactionManager))
                .next(createZelleVisaReconExceptionsStep(transactionManager))
                .listener(jobListener)  // Attach the job listener
                .build();
    }

    // Step 1: Zelle Visa Recon Exceptions Step
    @Bean
    @JobScope
    public Step zelleVisaReconExceptionsStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("zelleVisaReconExceptionsStep")
                .tasklet(zelleVisaReconExceptionsTasklet)
                .listener(promotionListener)  // Attach step listener
                .transactionManager(transactionManager)
                .build();
    }

    // Step 2: Create Zelle Visa Recon Exceptions Step
    @Bean
    @JobScope
    public Step createZelleVisaReconExceptionsStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("createZelleVisaReconExceptionsStep")
                .tasklet(createZelleVisaReconExceptionsTasklet)
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
    public ProcessReconExceptionsTasklet zelleVisaReconExceptionsTasklet(
            @Value("${zelle.mc.recon.missing.in.cash.query}") String missingInCashSql,
            @Value("${zelle.mc.recon.missing.in.dtb.query}") String missingInDtbSql,
            @Value("${batch.service.acct.plan.ref.query}") String acctPlanRefSql) {
        ProcessReconExceptionsTasklet tasklet = new ProcessReconExceptionsTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setMissingInCashSql(missingInCashSql);
        tasklet.setMissingInDtbSql(missingInDtbSql);
        tasklet.setAcctPlanRefSql(acctPlanRefSql);
        return tasklet;
    }

    @Bean
    @StepScope
    public CreateReconExceptionsTasklet createZelleVisaReconExceptionsTasklet(
            @Value("${cash.tran.except.insert}") String cashTranExceptInsert,
            @Value("${cash.tran.stat.except.insert}") String cashTranStatExceptInsert,
            @Value("${acct.ent.impacs.except.insert}") String acctEntImpacsExceptInsert) {
        CreateReconExceptionsTasklet tasklet = new CreateReconExceptionsTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setCashTranExceptInsert(cashTranExceptInsert);
        tasklet.setCashTranStatExceptInsert(cashTranStatExceptInsert);
        tasklet.setAcctEntImpacsExceptInsert(acctEntImpacsExceptInsert);
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
class ProcessReconExceptionsTasklet implements Tasklet {

    @Value("${zelle.mc.recon.missing.in.cash.query}")
    private String missingInCashSql;

    @Value("${zelle.mc.recon.missing.in.dtb.query}")
    private String missingInDtbSql;

    @Value("${batch.service.acct.plan.ref.query}")
    private String acctPlanRefSql;

    @Autowired
    private DataSource dataSource;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Tasklet logic here
        return RepeatStatus.FINISHED;
    }

    public void setDataSource(DataSource dataSource) { this.dataSource = dataSource; }
    public void setMissingInCashSql(String missingInCashSql) { this.missingInCashSql = missingInCashSql; }
    public void setMissingInDtbSql(String missingInDtbSql) { this.missingInDtbSql = missingInDtbSql; }
    public void setAcctPlanRefSql(String acctPlanRefSql) { this.acctPlanRefSql = acctPlanRefSql; }
}

@Component
@StepScope
class CreateReconExceptionsTasklet implements Tasklet {

    @Value("${cash.tran.except.insert}")
    private String cashTranExceptInsert;

    @Value("${cash.tran.stat.except.insert}")
    private String cashTranStatExceptInsert;

    @Value("${acct.ent.impacs.except.insert}")
    private String acctEntImpacsExceptInsert;

    @Autowired
    private DataSource dataSource;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Tasklet logic here
        return RepeatStatus.FINISHED;
    }

    public void setDataSource(DataSource dataSource) { this.dataSource = dataSource; }
    public void setCashTranExceptInsert(String cashTranExceptInsert) { this.cashTranExceptInsert = cashTranExceptInsert; }
    public void setCashTranStatExceptInsert(String cashTranStatExceptInsert) { this.cashTranStatExceptInsert = cashTranStatExceptInsert; }
    public void setAcctEntImpacsExceptInsert(String acctEntImpacsExceptInsert) { this.acctEntImpacsExceptInsert = acctEntImpacsExceptInsert; }
}
