package com.capg.batch;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
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

import com.capg.batch.model.Alien;
import com.capg.batch.processor.AlienItenProcessor;

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
	private Resource csvResource;

	@Autowired
	private DataSource dataSource;

	@Bean(name = "job1")
	public Job executeJob1() {

		Flow secondFlow = new FlowBuilder<Flow>("secondFlow").start(step2()).build();

		Flow parallelFlow = new FlowBuilder<Flow>("parallelFlow").start(step1()).split(new SimpleAsyncTaskExecutor())
				.add(secondFlow).build();

		return jobBuilderFactory.get("job1").incrementer(new RunIdIncrementer()).start(parallelFlow)
				/* .next(step2()) */.end().build();

	}

	@Bean(name = "job2")
	public Job executeJob2() {

		Flow secondFlow = new FlowBuilder<Flow>("secondFlow").start(step2()).build();

		Flow parallelFlow = new FlowBuilder<Flow>("parallelFlow").start(step1()).split(new SimpleAsyncTaskExecutor())
				.add(secondFlow).build();

		return jobBuilderFactory.get("job2").incrementer(new RunIdIncrementer()).start(parallelFlow).end()
				.build();

	}

	@Bean
	public Step step1() {

		return stepBuilderFactory.get("step1").<Alien, Alien>chunk(10).reader(reader1()).processor(processor())
				.writer(writer()) .taskExecutor(taskExecutor()) .build();

	}

	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2").<Alien, Alien>chunk(10).reader(reader1()).processor(processor())
				.writer(writer()).taskExecutor(taskExecutor()).build();
	}

//public TaskExecutor taskExecutor() {
//	SimpleAsyncTaskExecutor taskExecutor=new SimpleAsyncTaskExecutor();
//	taskExecutor.setConcurrencyLimit(5);
//		return taskExecutor ;
//	}
	// FOR RUNNING MULTIPLE JOBS IN PARALLEL
	@Bean
	public ThreadPoolTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(15);
		taskExecutor.setMaxPoolSize(20);
		taskExecutor.setQueueCapacity(30);
		return taskExecutor;
	}

	public FlatFileItemReader<Alien> reader1() {
		FlatFileItemReader<Alien> itemReader = new FlatFileItemReader<>();
		itemReader.setLineMapper(lineMapper1());
		itemReader.setLinesToSkip(1);
		itemReader.setResource(csvResource);
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

	public ItemProcessor<Alien, Alien> processor() {
		return new AlienItenProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Alien> writer() {
		JdbcBatchItemWriter<Alien> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO alien (aid,aname) VALUES (:aid, :aname)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Alien>());
		return itemWriter;
	}

}
