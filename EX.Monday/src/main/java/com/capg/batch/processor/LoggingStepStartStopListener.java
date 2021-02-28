package com.capg.batch.processor;

import java.util.List;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.AfterChunkError;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.core.annotation.OnReadError;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;

import com.capg.batch.model.Alien;

public class LoggingStepStartStopListener {

	@BeforeStep
	public void beforeStep(StepExecution stepExecution) {
		System.out.println(stepExecution.getStepName() + "Step has been started");
	}

	@AfterStep
	public ExitStatus afterStep(StepExecution stepExecution) {
		System.out.println(stepExecution.getStepName() + "step is ended");

		return stepExecution.getExitStatus();
	}

	@BeforeJob
	public void beforeJob(JobExecution jobExecution) {
		jobExecution.setExitStatus(ExitStatus.STOPPED);
		System.out.println("before job" + jobExecution.getId());
	}

	@AfterJob
	public void afterJob(JobExecution jobExecution) {
		System.out.println("after job" + jobExecution.getStatus());
	}

	@BeforeRead
	void beforeRead() {
		System.out.println("before reading the file");
	}

	@AfterRead
	void afterRead(Alien item) {
		System.out.println(item.getAname());

	}

	@OnReadError
	void onReadError(Exception ex) {
		System.out.println(ex.getMessage());
	}

	@BeforeChunk
	public void beforeChunk(ChunkContext context) {
		System.out.println("before the chunk:" + context.getStepContext());
	}

	@AfterChunk
	public void afterChunk(ChunkContext context) {
		System.out.println("after the chunk:" + context.getStepContext());
	}

}
