package com.example.demo;



import com.example.demo.entity.DynamicRecord;
import com.example.demo.reader.DynamicCsvReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DemoApplicationTests {

	@Mock
	private DynamicCsvReader reader;  // Mock the EmployeeReader

//	@BeforeEach
//	void setup() {
//		// Initialize mocks
//		MockitoAnnotations.openMocks(this);
//
//		// Mock the read() method to return Employee objects sequentially
//		when(reader.read()).thenReturn(
//				new DynamicRecord("E001", "John Doe", 50000.0),
//				new DynamicRecord("E002", "Jane Smith", 60000.0),
//				null  // To end the iteration, return null after all employees
//		);
//	}
//
//	@Test
//	void testEmployeeReading() {
//		// Call the method that will use the mocked EmployeeReader
//		DynamicRecord employee1 = reader.read();
//		DynamicRecord employee2 = reader.read();
//
//		// Validate that the mocked data is returned correctly
//		assertEquals("E001", employee1.getFields().get(0));
//		assertEquals("John Doe", employee1.getFields().get(1));
//
//		assertEquals("E002", employee2.getFields().get(0));
//		assertEquals("Jane Smith", employee2.getFields().get(1));
//
//		// No more employees, so this should return null
//		DynamicRecord employee3 = reader.read();
//		assertEquals(null, employee3);
//	}Test


	@Test
	void contextLoads() {
	}

}



