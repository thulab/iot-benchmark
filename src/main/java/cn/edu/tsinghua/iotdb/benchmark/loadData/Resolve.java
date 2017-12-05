package cn.edu.tsinghua.iotdb.benchmark.loadData;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.LinkedList;
import java.util.StringTokenizer;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;



public class Resolve implements Runnable{
	
	private File file;
	private static BufferedReader reader;
	private Config config;
	private Storage storage = null;
	private final int READ_LINES;
	
	public Resolve(String fileName,Storage storage){
		this.storage = storage;
		config = ConfigDescriptor.getInstance().getConfig();
		READ_LINES = config.BATCH_OP_NUM*config.DEVICE_NUMBER*2;
        try {
        	file = new File(fileName);
            reader = null;
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String handleFieldValue(String str) {
		// TODO Auto-generated method stub
		if(str.endsWith("i")){
			return str.substring(0,str.length()-1);		
		}
		else if(str.startsWith("\"")){
			return "'" + str.substring(1,str.length()-1) + "'";	
		}
		else if(str.startsWith("t")||str.startsWith("T")){
			return "true";
		}
		else if(str.startsWith("f")||str.startsWith("F")){
			return "false";
		}
		return str;
	}
	
	protected void finalize()
    {

		try {
			super.finalize();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// other finalization code...
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e1) {
			}
		}

    }

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			
			try {
				String tempString = null;
		        int line = 0;
		        LinkedList<String> prod =  new LinkedList<String>();
		        
		        // 读到指定的行数或者文件结束
		        while (line < READ_LINES && (tempString = reader.readLine()) != null) {
		        	
		        	Point p = new Point();
		        	
		            String[] pointInfo = tempString.split(" ");
		            StringTokenizer st = new StringTokenizer(pointInfo[0], ",=");
		            
		            //解析出measurement
		            if(st.hasMoreElements())
		            	p.measurement = st.nextToken().replace('.', '_');
		            
		            //解析出tag的K-V对
		            while(st.hasMoreElements()){  
		                p.tagName.add(st.nextToken().replace('.', '_'));
		                p.tagValue.add(st.nextToken().replace('.', '_').replace('/', '_'));
		            }
		            
		            //解析出field的K-V对
		            st = new StringTokenizer(pointInfo[1], ",=");
		            while(st.hasMoreElements()){  
		            	p.fieldName.add(st.nextToken().replace('.', '_'));
		                p.fieldValue.add(string2num(st.nextToken()));
		            } 

		            p.time = Long.parseLong(pointInfo[2].substring(0,pointInfo[2].length()-(19-13)));
		            line++;
		            prod.add(p.creatInsertStatement());
		            
		        }//while 
		        storage.produce(line, prod);
		        //读线程结束
		        if(tempString == null){
		        	storage.setEnd(true);
		        	break;
		        }
			} 
			catch (IOException e) {
				e.printStackTrace();
			} 
		}//while
		
	}//run
	
	private static Number string2num(String str) {
		// TODO Auto-generated method stub
		if(str.endsWith("i")){	
			return Long.parseLong(str.substring(0,str.length()-1));
		}
		else{
			return Double.parseDouble(str);
		}
	}

}
