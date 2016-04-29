import scala.io.Source

object RSI
{
    var avgLoss = 0.0;
    var avgGain = 0.0;
    var counter = 0.0;

    def getRSI( body: Double ) = 
    {
        counter = counter +1;
        var rsi = -1.0;
        if( counter < 14 )
        {
            if( body < 0 )
                avgLoss = avgLoss + math.abs(body);
            else
                avgGain = avgGain + math.abs(body);      
        }
        else
        {
           counter = 15;
           if( body > 0 )
               avgGain = ( avgGain * 13 + body) /14.0;
           else
               avgLoss = (avgLoss *13 + math.abs(body))/14.0;

           val rs = avgGain / avgLoss;
           rsi = 100 - (100 /(rs + 1));
        }
        rsi
    }

}
object ParseForModel
{
	/* Please note that each input file line once splitted on ',' delimiter is like this:
       0 -> hours, 1 -> Mins, 2 -> Open, 3 -> High, 4 -> Low, 5 -> Close
    */
    val indexHour = 0
    val indexMin = 1
    val indexOpen = 2
    val indexHigh = 3
    val indexLow = 4
    val indexClose = 5
    val pointForPips = 10000
    val pipsThreshold = 60
    val howAhead = 4 
    var counter = 0
    val howPast = 4
    val rsi = RSI
	
	/* lowAhead and highAhead are going to contain low and high of the 'howAhead' ahead lines */
	var lowAhead : Array[Double] = Array()
	var highAhead :Array[Double] = Array()
	var closeAhead :Array[Double] = Array()
	var pastLowInPips : Array[Int] = Array()
	var pastHighInPips : Array[Int] = Array()
	var pastBodyInPips : Array[Int] = Array()


    def isLondonOpen(hour: Double) =
    {
	println("___________")
	println(hour)
	if (hour == 8.0)
        {
	    true;
	}else{
		false;
	}

    }
    def getMarketPips( close : Double ) = 
    {
    	val pips = 
    	{
    		closeAhead.map( x => 
    					{
    						val diffInPips = pointForPips* ( close - x ) toInt; 
    						diffInPips
    					}
    					).reduce(_+_)

    	}
    	pips
    }
    def getMarket( close : Double ) = 
    {
    	val sell = 
    	{
    		lowAhead.map( low => 
    					{
    						val diffInPips = pointForPips* ( close - low ) toInt; 
    						if( diffInPips > pipsThreshold )
    						    1
    						else
    						    0
    					}
    					).reduce(_+_)

    	}
    	val buy = 
    	{
    		highAhead.map{ high => 
    						val diffInPips = pointForPips* ( high -  close ) toInt;
    						if( diffInPips > pipsThreshold )
    						    1
    						else
    						    0
    					}.reduce(_+_)

    	}
    	if( (sell > 0 && buy > 0) || (sell == 0 && buy == 0) )
    	    0
    	else if( sell > 0)
    	       2
    	else 1
    
    }
	/* Given an input file line, returns true if
	   it represents a bearish candle 
	*/
	def isBearish( line: Array[Double] ) = 
	{
		val open = line(indexOpen)
		val close = line(indexClose)
		if( open >= close )
		    true
		false
	}
	/* Given an input file line, returns low in Pips*/
	/* To do that, we need to understand if it is a bullish or bearish candle
	   if bullish (green candle) then compute open - low  
	   if bearish (red candle ) then compute  close - low
	*/ 
	def getLow( line: Array[Double] ) = 
	{
		val isBear = isBearish(line)
        val open = line(indexOpen)
        val close = line(indexClose)
        val high = line(indexHigh)
        val low = line(indexLow)
        if( isBear == true )
        {
        	pointForPips* ( math.abs( close - low )) toInt 
        }
        else
            pointForPips* ( math.abs( open - low )) toInt
        
	}

	/* Given an input file line, returns high in Pips 
	   To do that, we need to understand if it is a bullish or bearish candle
	   if bullish (green candle) then compute high - close 
	   if bearish (red candle ) then compute high - open 
	*/
	def getHigh( line: Array[Double] ) = 
	{
        val isBear = isBearish(line)
        val open = line(indexOpen)
        val close = line(indexClose)
        val high = line(indexHigh)
        if( isBear == true )
        {
        	pointForPips* ( math.abs( high - open )) toInt 
        }
        else
            pointForPips* ( math.abs( high - close )) toInt

	}
	/* Given an input file line, return body in pips
	   To do that, you can compute close - open, which is 
	   positive if we have a bullish candle, negative else
	*/
	def getBody( line: Array[Double]) =
	{   
        val open = line(indexOpen)
        val close = line(indexClose)
        pointForPips* ( ( close - open )) toInt
	}

    
	def main( args: Array[String] ) = 
	{
		val usage = "scala ParseForModel candleStickFile "

		if( args.size != 1 )
		{
			println("Error in args usage is: \n" + usage)
			sys.exit(-1)
		}
		val inputFile = args(0)

		/* Open file in read mode */
		val lineI = Source.fromFile(inputFile ).getLines()

		/* Here, I am going to loop in the inputFile, lineAhead and lineI refer to the same file
		   but lineAhead is 'howAhead' lines ahead of lineI
		*/ 
		for( lineAhead <- Source.fromFile(inputFile ).getLines())
    	{
 			 counter = counter + 1
 			 val arrayLine = lineAhead.split(",").map(_.toDouble)
 			 val currentLow = arrayLine(indexLow)
 			 val currentHigh = arrayLine(indexHigh)
			 val currentClose = arrayLine(indexClose)
 			 lowAhead = lowAhead  :+ currentLow
 			 highAhead = highAhead :+ currentHigh
			 closeAhead = closeAhead :+ currentClose 
 			 println("LowAhead Size " + lowAhead.size)
 			 if( lowAhead.size > howAhead && highAhead.size > howAhead )
 			 {
 			 	 lowAhead = lowAhead.drop(1) 
 			 	 highAhead = highAhead.drop(1)
				 closeAhead = closeAhead.drop(1)
 			 }  
 			 if( counter > howAhead )
 			 {
 			 	 /* If here currentLine refers to the i-th line,
 			 	    while lineAhead refers to the (i + howAhead) line 
 			 	    and low/high-Ahead hold the [ i +1 , i + howAhead] low and high info
 			 	 */ 
 			 	 counter = howAhead + 1
 			 	 val currentLine = lineI.next() 
 			 	 val arrayLineC = currentLine.split(",").map(_.toDouble)
 			 	 println("CurrentLine " + currentLine)
 			 	 println("lowAhead ")
 			 	 lowAhead.foreach(println)
 			 	 val open = arrayLineC(indexOpen)
 			 	 val hour = arrayLineC(indexHour)
 			 	 val low = arrayLineC(indexLow)
 			 	 val high = arrayLineC(indexHigh)
 			 	 val close = arrayLineC(indexClose)
 			 	 //Compute low,high,body in pips for the current candle
 			 	 val lowInPips = getLow(arrayLineC)
 			 	 val highInPips = getHigh(arrayLineC)
 			 	 val bodyInPips = getBody(arrayLineC)
 			 	 //Insert in the correct array
 			 	 pastBodyInPips = pastBodyInPips :+ bodyInPips
 			 	 pastHighInPips = pastHighInPips :+ highInPips
 			 	 pastLowInPips = pastLowInPips  :+ lowInPips
 			 	 val currentRSI = rsi.getRSI(bodyInPips )

 			 	 if( pastBodyInPips.size > howPast )
 			 	 {
 			 	 	 pastLowInPips = pastLowInPips.drop(1)
 			 	 	 pastBodyInPips = pastBodyInPips.drop(1)
 			 	 	 pastHighInPips = pastHighInPips.drop(1)
 			 	 }
 			 	 if( currentRSI != -1 )
 			 	 {
 	  			 	 val market = getMarket(close)
					 if ( isLondonOpen(hour)) 
					 {
 	  			 	      val stringFeatures = hour + "," + pastLowInPips.mkString(",") + "," + pastBodyInPips.mkString(",") + "," +
 	  			 	                      pastHighInPips.mkString(",")

 	  			 	      val stringToPrint = market + "," + stringFeatures
 	  			 	      scala.tools.nsc.io.File(inputFile + "_Model").appendAll(stringToPrint + "\n");
					 }

 			 	 }
 			 	 


 			 }
 			 

    	}
	}
}
