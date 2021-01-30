package com.capg.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.capg.batch.model.Alien;

public class AlienItemProcessor  implements ItemProcessor<Alien, Alien>{

	@Override
	public Alien process(Alien item) throws Exception {
		System.out.println("alien data"+item);
		return item;
	}

}
