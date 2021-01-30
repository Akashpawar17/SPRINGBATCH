package com.capg.batch.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.capg.batch.processor.AlienItemProcessor;

@EnableBatchProcessing
@Component
public class AlienBatchConfig {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Value("/AAAAAAAAAA/src/main/resources/csv/alien.csv")
	private Resource csvResource;
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public  Job process() {
		return jobBuilderFactory.get("job").flow(step1()).end().build();
		
	}

	private Step step1() {
		// TODO Auto-generated method stub
		return stepBuilderFactory.get("step").<Alien,Alien>chunk(10).reader(reader()).processor(processor()).writer(writer()).build();
	}


public  JdbcCursorItemReader<Alien> reader(){
	JdbcCursorItemReader<Alien> reader=new JdbcCursorItemReader<>();
	reader.setDataSource(dataSource);
	reader.setSql("Select aid,aname from alien");
	reader.setRowMapper(new UserRowMapper());
	return reader;
	
}
public class UserRowMapper implements RowMapper<Alien>{

	@Override
	public Alien mapRow(ResultSet rs, int rowNum) throws SQLException {
		Alien a=new Alien();
		a.setAid(rs.getInt("aid"));
		a.setAname(rs.getString("aname"));
		return a;
	}
	
}
public AlienItemProcessor processor() {
	return new AlienItemProcessor();
	
}
public FlatFileItemWriter<Alien> writer(){
	
	FlatFileItemWriter<Alien> writer=new FlatFileItemWriter<>();
	writer.setResource(csvResource);
	writer.setAppendAllowed(true);
	writer.setLineAggregator(new DelimitedLineAggregator<Alien>(){
	
	{
	setDelimiter(",");
	setFieldExtractor(new BeanWrapperFieldExtractor<Alien>() {
		{
			setNames(new String[] {"aid","aname"});
		}
	});
	}
	});

	return writer;
	
}
	

}
