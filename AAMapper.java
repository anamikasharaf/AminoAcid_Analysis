package lab2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class AAMapper  extends Mapper <LongWritable,Text,Text,Text> {
    
    Map<String, String> codon2aaMap = new HashMap<String, String>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] codon2aaPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(codon2aaPath != null && codon2aaPath.length > 0) {
                codon2aaMap = readFile(codon2aaPath);
            }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
                System.exit(1);
            }
        }
    
    protected HashMap<String, String> readFile(Path[] codonFilePath) {
        HashMap<String, String> codonMap = new HashMap<String, String>();
        BufferedReader cacheReader=null;
        String line=null;
        String[] lineArray=null;
        try{
           cacheReader = new BufferedReader(new FileReader(codonFilePath[0].toString()));
           while((line=cacheReader.readLine())!=null) {
               // Isoleucine      I       ATT, ATC, ATA
                 lineArray = line.split("\\t");
                 String aminoAcid = lineArray[0];
                 String[] sequencesArray = lineArray[2].split(",");
                 for(String sequence: sequencesArray) {
                     codonMap.put(sequence.trim(), aminoAcid.trim());
                 }
           }
        }
        catch(Exception e) { 
            e.printStackTrace(); 
            System.exit(1);
        }
        return codonMap;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {
    	
        // TODO declare String variable for Text value
    	String text = value.toString();
        
        // TODO: declare and initialize variables for tokens of each of 3 reading frames
    	StringTokenizer st1 = new StringTokenizer(text, "\n");
    	StringTokenizer st2 = new StringTokenizer(text.substring(1), "\n");
    	StringTokenizer st3 = new StringTokenizer(text.substring(2), "\n");
    	
  
        // TODO: read, process, and write reading frame 1
        // TCA GCC TTT TCT TTG ACC TCT TCT TTC TGT TCA TGT GTA TTT GCT GTC TCT TAG CCC AGA
        // does TCA exist in codon2aaMap?
        // if so, write (key, value) pair to context   
    	setup(context);
    	String frame1;
    	
    	while(st1.hasMoreTokens())
    	{
    		frame1 = st1.nextToken().toString();
    		
    		for (int i=0; i< frame1.length()-1; i+=3)
    		{
    			if(codon2aaMap.containsKey(frame1.substring(i, i+3)))
    			{
    				String ProteinFound = codon2aaMap.get(frame1.substring(i, i+3));
    				context.write(new Text(ProteinFound), new Text("Frame1_1"));
    			}
    		}
    	}
    	

        // TODO: read, process, and write reading frame 2
        // T CAG CCT TTT CTT TGA CCT CTT CTT TCT GTT CAT GTG TAT TTG CTG TCT CTT AGC CCA GA
        // does CAG exist in codon2aaMap?
        // if so, write (key, value) pair to context 
    	String frame2;
    	while(st2.hasMoreTokens())
    	{
    		frame2 = st2.nextToken().toString();
    		
    		for (int i=0; i< frame2.length()-3; i+=3)
    		{
    			if(codon2aaMap.containsKey(frame2.substring(i, i+3)))
    			{
    				String ProteinFound = codon2aaMap.get(frame2.substring(i, i+3));
    				context.write(new Text(ProteinFound), new Text("Frame2_1"));
    			}
    		}
    	}
        
        // TODO: read, process, and write reading frame 3
        // TC AGC CTT TTC TTT GAC CTC TTC TTT CTG TTC ATG TGT ATT TGC TGT CTC TTA GCC CAG A
        // does AGC exist in codon2aaMap?
        // if so, write (key, value) pair to context 
    	
    	String frame3;
    	while(st3.hasMoreTokens())
    	{
    		frame3 = st3.nextToken().toString();
    		
    		for (int i=0; i< frame3.length()-2; i+=3)
    		{
    			if(codon2aaMap.containsKey(frame3.substring(i, i+3)))
    			{
    				String ProteinFound = codon2aaMap.get(frame3.substring(i, i+3));
    				context.write(new Text(ProteinFound), new Text("Frame3_1"));
    			}
    		}
    	}
       
    }
}