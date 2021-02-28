package com.capg.batch.processor;

import java.util.HashMap;
import java.util.Map;

import javax.batch.runtime.StepExecution;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Component
@EnableScheduling
public class MyScheduler {

	@Autowired
	private JobLauncher jobLauncher;

	/*
	 * @Autowired private Job job;
	 */

	@Autowired
	private Job job1;


	/*
	 * @Autowired
	 * 
	 * @Qualifier("job2") private Job job2;
	 */

	
	
	@Scheduled(cron = "0/15 * * * * ?")
	public void perform1() throws Exception {
		JobParameters para = new JobParametersBuilder().addString("jobparameter", String.valueOf(System.nanoTime()))
				.toJobParameters();

		JobParameters params = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();
		
		JobExecution jobExecution = jobLauncher.run(job1, params);
		
		
		
		  if (jobExecution.getStatus() == BatchStatus.COMPLETED ) {
		  System.out.println("job status:" + jobExecution.getJobId() + ":" + ":" +
		  jobExecution.getStartTime() + ":" + jobExecution.getEndTime() + ":" +
		  jobExecution.getStatus()+":"+jobExecution.getExitStatus());
		  
		  
		  }
		 
			  else if (jobExecution.getStatus() == BatchStatus.FAILED) {
			  System.out.println("job has failed");
			  }
		
	}

	/*
	 * @Scheduled(cron = "0/15 * * * * ?") public void perform2() throws Exception {
	 * 
	 * JobParameters params = new JobParametersBuilder().addString("JobID",
	 * String.valueOf(System.currentTimeMillis())) .toJobParameters();
	 * jobLauncher.run(job2, params); }
	 */
}
