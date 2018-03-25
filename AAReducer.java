package lab2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

public class AAReducer  extends Reducer <Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
        
        // initialize integer sums for each reading frame
    	int frame1_sum = 0;
    	int frame2_sum = 0;
    	int frame3_sum = 0;
        
        // loop through Iterable values and increment sums for each reading frame
    	String[] valarray;
    	String valstring;
    	for(Text val : values)
    	{
    		valstring = val.toString();
    		valarray = valstring.split("_");
    		if(valarray[0].equals("Frame1"))
    		{
    			frame1_sum = frame1_sum + 1;
    		}
    		else if(valarray[0].equals("Frame2"))
    		{
    			frame2_sum = frame2_sum + 1;
    		}
    		else
    		{
    			frame3_sum = frame3_sum + 1;
    		}
    	}
       
        // write the (key, value) pair to the context
    	
    	String finalvalue = Integer.toString(frame1_sum) + " " + Integer.toString(frame2_sum) + " " + frame3_sum;
    	if(key.toString().length() <= 7)
    	{
    		context.write(key, new Text("\t\t\t" + finalvalue));
    	}
    	else
    	{
    		context.write(key, new Text("\t\t" + finalvalue));
    	}
               
	  
   }
}
