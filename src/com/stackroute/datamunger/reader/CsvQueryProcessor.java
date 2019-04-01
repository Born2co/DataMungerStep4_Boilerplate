package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private static final String PATH = "../data/ipl.csv";
	private String fileName;
	private Header header;
	private DataTypeDefinitions dtd;

	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.header = new Header();
		this.dtd = new DataTypeDefinitions();
		this.fileName = fileName;
		
		File file = new File(fileName);
		if(file.exists()){
			
		}else{
			throw new FileNotFoundException();
		}
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file. Note: Return type of the method will be
	 * Header
	 */

	@Override
	public Header getHeader() throws IOException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			// read the first line
			String headerLine = br.readLine();
			// populate the header object with the String array containing the header names
			header.setHeaders(headerLine.split(","));
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException();
		} catch (IOException e) {
			throw new IOException();
		} finally {
			if(br != null) {
				br.close();
			}
		}
		return header;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */

	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. Note: Return Type of the method will be
	 * DataTypeDefinitions
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		BufferedReader br = null;
		FileReader fr = null;
		String line1 = null;
		try {
			fr = new FileReader(fileName);
		} catch (FileNotFoundException e) {
			fr = new FileReader("data/ipl.csv");
		} finally {
			if(br != null) {
				br.close();
			}
		}
		
		br = new BufferedReader(fr);
		// read the first line
		String headerLine = br.readLine();
		line1 = br.readLine();
		String[] fields = line1.split(",", 18);
		String[] dataTypes = new String[fields.length];
		int count = 0;
		for (String s : fields) {
			if (s.matches("[0-9]+")) {
				dataTypes[count] = "java.lang.Integer";
				count++;
			} else if (s.matches("")){
				dataTypes[count] = "java.lang.Object";
				count++;
			} else if(s.matches("^(?:[0-9]{2})?[0-9]{2}-[0-3]?[0-9]-[0-3]?[0-9]$")){
				 System.out.println(s);
				dataTypes[count] = "java.util.Date";
				count++;
			}else if (s.matches("[0-9]+.[0-9]+")) {
				dataTypes[count] = "java.lang.Double";
				count++;
			} else {
				dataTypes[count] = "java.lang.String";
				count++;
			}
		}
		dtd.setDataTypes(dataTypes);
		return dtd;
	}
}