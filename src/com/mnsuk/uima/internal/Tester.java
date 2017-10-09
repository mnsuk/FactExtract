package com.mnsuk.uima.internal;

import com.mnsuk.uima.FactExtract;


public class Tester {
	public static void main(String[] args) {
		String fileType1 = "/Users/martin/Documents/Development/Java/Annotators/FactExtract/data/emp-ts.xml";  // _InitialView
		String CASFile1 = "/Users/martin/Documents/Development/Java/Annotators/FactExtract/data/emp.xmi";
		
		
		FactExtract anno = new FactExtract();
		FactExtract.setpDBHost("192.168.2.70");
		FactExtract.setpDBPort("3306");
		FactExtract.setpDBName("EXTRACTS");
		FactExtract.setpDBSchema("DEMO");
		FactExtract.setpDBUser("root");
		FactExtract.setpDBPassword("admin");
		FactExtract.setpDB("MYSQL");
		FactExtract.setpProject("EMP");
		FactExtract.setpDrop(true);
		FactExtract.setpPrependTableNames(true);
		FactExtract.setpSaveDocText(true);
		FactExtract.setpLazymode(false);
		FactExtract.setpKeyField("default");  // usually _InitialView or lrw-view

		anno.TestAnnotator( fileType1, CASFile1);

	}
}
