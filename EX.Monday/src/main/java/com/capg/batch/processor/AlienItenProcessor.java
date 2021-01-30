package com.capg.batch.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.capg.batch.model.Alien;



public class AlienItenProcessor  implements ItemProcessor<Alien, Alien>{
	

	
  public Alien process(Alien employee) throws Exception
  {
 System.out.println("data inserted into database"+employee);//+""+Thread.currentThread().getId());
  	  return employee;
  }
	
}
