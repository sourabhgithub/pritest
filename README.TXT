import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.beans.factory.annotation.Autowired;
import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@PropertySource("classpath:application.properties")  // Load properties from application.properties
public class DatabaseConfig {

    @Autowired
    private Environment env;

    // Bean definition for EncryptedDataSource
    @Bean
    public DataSource encryptedDataSource() {
        EncryptedDataSource dataSource = new EncryptedDataSource();
        dataSource.setDriverClassName(env.getProperty("db.driverClass"));
        dataSource.setUrl(env.getProperty("db.connectionURL"));
        dataSource.setUsername(env.getProperty("db.username"));
        dataSource.setPassword(env.getProperty("db.password"));
        return dataSource;
    }

    // Bean definition for EntityManagerFactory
    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
        LocalContainerEntityManagerFactoryBean entityManagerFactory = new LocalContainerEntityManagerFactoryBean();
        entityManagerFactory.setDataSource(encryptedDataSource());
        entityManagerFactory.setPackagesToScan("com.baml.cash");  // Adjust the package to your entity package

        Properties jpaProperties = new Properties();
        jpaProperties.put("hibernate.dialect", "org.hibernate.dialect.Oracle12cDialect");
        jpaProperties.put("javax.persistence.schema-generation.database.action", "none");
        jpaProperties.put("hibernate.ejb.use_class_enhancer", "true");
        jpaProperties.put("hibernate.show_sql", "true");

        entityManagerFactory.setJpaProperties(jpaProperties);
        entityManagerFactory.setPersistenceProviderClass(org.hibernate.jpa.HibernatePersistenceProvider.class);
        return entityManagerFactory;
    }

    // Bean definition for JPA Transaction Manager
    @Bean
    public PlatformTransactionManager transactionManager() {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(entityManagerFactory().getObject());
        return transactionManager;
    }
}
