import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;

public class fromTickToTimeFrame {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
        String inputFile = "EURUSD_2009To2015";
		int period = 15;
		String windows = "1H";
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		PrintWriter out = new PrintWriter(inputFile + "_" + windows );		

		String line= "";
		int counter = 0;
		double high = 0;
		double close = 0;
		double low = 11111110;
		String open = "";
		double init_time = 0;
		Timestamp init = null;
		long timeFrame = 1000 * 60 * 60 * 1;
		// EUR/USD,20110811 10:34:00.306,1.4187,1.4188
		while( ( line = br.readLine() )!= null)
		{
			String time_tmp = line.split(",")[1]; // 20110811 10:34:00.306
			time_tmp = time_tmp.substring(0, 4) + "-" + time_tmp.substring(4,6) + "-" + time_tmp.substring(6);
			Timestamp timestamp = Timestamp.valueOf(time_tmp);
			time_tmp = time_tmp.split(":")[1]; //minutes

			double current_price = Double.parseDouble(line.split(",")[2]);
			if( counter == 0)
			{
				init_time = Double.parseDouble(time_tmp);
				init = timestamp;
				open = line.split(",")[2];
			}
			counter ++;
			if( high < current_price)
				high = current_price;
			if( low > current_price)
				low = current_price;

			String tmp_ = line.split(",")[1].split(" ")[1];
            		String hour_ = tmp_.split(":")[0]; 

			double current_time = Double.parseDouble(time_tmp);
			if( Math.abs(timestamp.getTime() - init.getTime()) >= (timeFrame) )
			{
				counter = 0;
				close = current_price;
                //System.out.println(line);
                String tmp = line.split(",")[1].split(" ")[1];
                String hour = tmp.split(":")[0]; 
                String minute = tmp.split(":")[1];
				out.write(Integer.parseInt(hour) + "," + Integer.parseInt(minute) + ","  + open + "," + high + "," + low + "," + close);
				out.write("\n");
				high = 0;
				low = 9999999;

			}
			
		}
		br.close();
		out.close();
	}

}
