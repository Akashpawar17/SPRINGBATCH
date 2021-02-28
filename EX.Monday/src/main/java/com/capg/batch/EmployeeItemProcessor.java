package com.capg.batch;

import org.springframework.batch.item.ItemProcessor;

import com.capg.batch.model.Employee;

public class EmployeeItemProcessor implements ItemProcessor<Employee, Employee> {

	@Override
	public Employee process(Employee item) throws Exception {
		// TODO Auto-generated method stub
		return item;
	}

}
