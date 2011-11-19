package uk.ac.shef.wit.simmetrics.similaritymetrics;

import java.io.UnsupportedEncodingException;

public class test {

	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		AbstractStringMetric metric = new JaroWinkler();
		float result = metric.getSimilarity(new String("许瑞".getBytes(),"utf-8"), new String("许瑞".getBytes(),"utf-8"));
		System.out.print(result);
	}

}
