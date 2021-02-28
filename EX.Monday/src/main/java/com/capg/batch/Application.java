package com.capg.batch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
//import org.springframework.batch.integration.async.AsyncItemProcessor;
//import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.capg.batch.model.Alien;
import com.capg.batch.model.Employee;
import com.capg.batch.processor.AlienItenProcessor;
import com.capg.batch.processor.LoggingStepStartStopListener;

@SpringBootApplication
@EnableBatchProcessing
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Value("classPath:/csv/alien.csv")
	private Resource csvResource1;
	@Value("classPath:/csv/employee.csv")
	private Resource csvResource2;
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public Job executeJob1() throws Exception {

		/*
		 * Flow secondFlow = new FlowBuilder<Flow>("secondFlow").start(step2()).build();
		 * 
		 * Flow parallelFlow = new
		 * FlowBuilder<Flow>("parallelFlow").start(step1()).split(new
		 * SimpleAsyncTaskExecutor()) .add(secondFlow).build();
		 */
		return jobBuilderFactory.get("job1").incrementer(new RunIdIncrementer()).flow(step1()).end().build();
				//.on("COMPLETED").to(step2()).from(step2()).on("COMPLETED").to(step3()).from(step3()).end().build();
			

	}

	

	private JobParametersIncrementer SampleIncrementer() {
		JobParametersIncrementer incre=new JobParametersIncrementer() {
			
			@Override
			public JobParameters getNext(JobParameters parameters) {
				 long id = parameters.getLong("run.id",1L);
				  
				  return new JobParametersBuilder().addLong("run.id", id).toJobParameters();
					  
					  
	
			}
		};
		return incre;
		
	}
	



	/*
	 * @Bean(name = "job2") public Job executeJob2() {
	 * 
	 * Flow secondFlow = new FlowBuilder<Flow>("secondFlow").start(step2()).build();
	 * 
	 * Flow parallelFlow = new
	 * FlowBuilder<Flow>("parallelFlow").start(step1()).split(new
	 * SimpleAsyncTaskExecutor()) .add(secondFlow).build();
	 * 
	 * return jobBuilderFactory.get("job2").incrementer(new
	 * RunIdIncrementer()).start(parallelFlow).end() .build();
	 * 
	 * }
	 */

	@Bean
	public Step step1() throws Exception {

		return stepBuilderFactory.get("step1").allowStartIfComplete(true).<Alien, Alien>chunk(50).reader(reader1())
				.processor(processor1()).writer(writer1()).allowStartIfComplete(true).
				listener(new LoggingStepStartStopListener()).
				build();

	}

	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2").<Employee,Employee>chunk(10).reader(reader2()).processor(processor2())
				.writer(writer2()).build();
	}
	@Bean
	public Step step3() {
		return stepBuilderFactory.get("step3").<Employee,Employee>chunk(10).reader(reader2()).processor(processor2())
				.writer(writer2()).build();
	}

//public TaskExecutor taskExecutor() {
//	SimpleAsyncTaskExecutor taskExecutor=new SimpleAsyncTaskExecutor();
//	taskExecutor.setConcurrencyLimit(5);
//		return taskExecutor ;
//	}
	// FOR RUNNING MULTIPLE JOBS IN PARALLEL
	/*
	 * @Bean public ThreadPoolTaskExecutor taskExecutor() { ThreadPoolTaskExecutor
	 * taskExecutor = new ThreadPoolTaskExecutor();
	 * taskExecutor.setCorePoolSize(15); taskExecutor.setMaxPoolSize(20);
	 * taskExecutor.setQueueCapacity(30); return taskExecutor; }
	 */

	public FlatFileItemReader<Alien> reader1() {
		FlatFileItemReader<Alien> itemReader = new FlatFileItemReader<>();
		itemReader.setLineMapper(lineMapper1());
		itemReader.setLinesToSkip(1);
		itemReader.setResource(csvResource1);
		return itemReader;
	}

// convert csv rows to beans

	public LineMapper<Alien> lineMapper1() {
		DefaultLineMapper<Alien> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames(new String[] { "aid", "aname" });
		lineTokenizer.setIncludedFields(new int[] { 0, 1 });
		BeanWrapperFieldSetMapper<Alien> fieldSetMapper = new BeanWrapperFieldSetMapper<Alien>();
		fieldSetMapper.setTargetType(Alien.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}
	public FlatFileItemReader<Employee> reader2() {
		FlatFileItemReader<Employee> itemReader = new FlatFileItemReader<>();
		itemReader.setLineMapper(lineMapper2());
		itemReader.setLinesToSkip(1);
		itemReader.setResource(csvResource2);
		return itemReader;
	}

// convert csv rows to beans

	public LineMapper<Employee> lineMapper2() {
		DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames(new String[] { "eid", "ename", "salary" });
		lineTokenizer.setIncludedFields(new int[] { 0, 1 ,2});
		BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<Employee>();
		fieldSetMapper.setTargetType(Employee.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	public ItemProcessor<Employee, Employee> processor2() {
		return new EmployeeItemProcessor();

	}
	public ItemProcessor<Alien, Alien> processor1() {
		return (alien) -> {
			Thread.sleep(1);
			return alien;
		};

	}
	
	/*
	 * public AsyncItemProcessor<Alien, Alien> asyncItemProcessor() throws Exception
	 * { AsyncItemProcessor<Alien, Alien> processor = new AsyncItemProcessor<>();
	 * 
	 * processor.setDelegate(processor()); processor.setTaskExecutor(new
	 * SimpleAsyncTaskExecutor());
	 * 
	 * return processor; }
	 * 
	 * public AsyncItemWriter<Alien> asyncItemWriter() throws Exception {
	 * AsyncItemWriter<Alien> writer = new AsyncItemWriter<>();
	 * 
	 * writer.setDelegate(writer());
	 * 
	 * return writer; }
	 */

	@Bean
	public JdbcBatchItemWriter<Alien> writer1() {
		JdbcBatchItemWriter<Alien> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO alien (aid,aname) VALUES (:aid, :aname)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Alien>());
		return itemWriter;
	}
	@Bean
	public JdbcBatchItemWriter<Employee> writer2() {
		JdbcBatchItemWriter<Employee> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO Employee (eid,ename,salary) VALUES (:eid, :ename, :salary)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
		return itemWriter;
	}

	}
