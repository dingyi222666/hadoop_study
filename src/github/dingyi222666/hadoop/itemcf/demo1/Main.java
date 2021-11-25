package github.dingyi222666.hadoop.itemcf.demo1;

import github.dingyi222666.hadoop.itemcf.demo1.*;

public class Main {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		
		
		Class[] classes = new Class[] {Step1.class,Step2.class,Step3.class,Step4.class,Step5.class,Step6.class};
		
		for (Class class1 : classes) {
			class1.getMethod("main",null)
			.invoke(null, null);
		}

	}

}
