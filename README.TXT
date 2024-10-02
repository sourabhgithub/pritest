@Configuration
@EnableBatchProcessing
public class BatchJobConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    // Job definition
    @Bean
    public Job cashExceptionsProcessJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return jobBuilderFactory.get("cashExceptionsProcessJob")
                .start(dtbReconExceptionsStep(transactionManager))
                .next(createDtbReconExceptionsStep(transactionManager))
                .listener(jobListener())  // Attach a listener to the job
                .build();
    }

    // Step 1 definition with step scope
    @Bean
    @StepScope  // Step-scoped bean for tasklet
    public Step dtbReconExceptionsStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("dtbReconExceptionsStep")
                .tasklet(dtbReconExceptionsTasklet())
                .listener(promotionListener())
                .transactionManager(transactionManager)
                .build();
    }

    // Step 2 definition with step scope
    @Bean
    @StepScope  // Step-scoped bean for tasklet
    public Step createDtbReconExceptionsStep(PlatformTransactionManager transactionManager) {
        return stepBuilderFactory.get("createDtbReconExceptionsStep")
                .tasklet(createDtbReconExceptionsTasklet())
                .listener(promotionListener())
                .transactionManager(transactionManager)
                .build();
    }

    // Tasklet for Step 1 with Step Scope
    @Bean
    @StepScope  // Step-scoped tasklet bean
    public ProcessReconExceptionsTasklet dtbReconExceptionsTasklet() {
        ProcessReconExceptionsTasklet tasklet = new ProcessReconExceptionsTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setAcctPlanRefSql("${batch.service.acct.plan.ref.query}");
        return tasklet;
    }

    // Tasklet for Step 2 with Step Scope
    @Bean
    @StepScope  // Step-scoped tasklet bean
    public CreateReconExceptionsTasklet createDtbReconExceptionsTasklet() {
        CreateReconExceptionsTasklet tasklet = new CreateReconExceptionsTasklet();
        tasklet.setDataSource(dataSource);
        tasklet.setCashTranStatExceptInsertSql("${cash.tran.stat.except.insert}");
        return tasklet;
    }

    // Listener definition
    @Bean
    @JobScope  // Job-scoped listener bean
    public CashExceptionsBatchJobListener jobListener() {
        return new CashExceptionsBatchJobListener();
    }

    // Execution Context Promotion Listener
    @Bean
    @StepScope
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{"domainDataKey", "cashPayloadData", "exceptTranList"});
        return listener;
    }
}
