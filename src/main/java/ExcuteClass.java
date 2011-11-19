import org.apache.hadoop.util.ProgramDriver;


public class ExcuteClass {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		 ProgramDriver pgd = new ProgramDriver();  
		    try {  
		      pgd.addClass("test", Test.class, "A map/reduce program that analysis log .");  
		      pgd.driver(args);  
		    }  
		    catch(Throwable e){  
		      e.printStackTrace();  
		  
		    }  
	}

}
