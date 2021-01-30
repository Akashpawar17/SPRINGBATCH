package com.capg.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class AaaaaaaaaaApplication {

	public static void main(String[] args) {
		SpringApplication.run(AaaaaaaaaaApplication.class, args);
	}
	
	@Autowired
	private JobLauncher jobLauncher;
	
	  @Autowired private Job job;
	 
	
	
	
	@Scheduled(fixedRate = 5000)
	public void execute1() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		
		JobParameters paramater=new JobParametersBuilder().addString("job1", String.valueOf(System.currentTimeMillis())).toJobParameters();
	
	
	jobLauncher.run(job, paramater);
	}
	
}
