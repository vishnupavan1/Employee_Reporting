package com.bigdata.ex

object driver {

  def main(args: Array[String]): Unit = {

    /* Setting system properties */
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    
    /*
     * creating an object to intiate the process 
     * 
     * This sample project has two methods 
     * 1.Initialize --> This will create the spark session --> Entry point for spark execution 
     * 2.read --> This will read the source data and outputs the dataframe
     * 3.Process --> This method will process the bussiness logic and outputs the dataframe
     * 4.write --> This method will write the data to the requried path
     * we can use property file --> which will contain the process name , and class name to run we can read the property file
     * and parse the arguments and trigger the code for different data sources
     * 
     *  */

    var class_name:String = ""

    def parse_arguments(a:Array[String]) = {

        var i = 0

      for (i <- 0 to a.length - 1 ) {

         class_name =  "com.bigdata.ex.".concat(a(i))

      }

    }
    parse_arguments(args)
    val process_obj = Class.forName(class_name).newInstance().asInstanceOf[com.bigdata.ex.Transformation]
    
    /*
     * 
     * 
     * */
    
    process_obj.initialize()
    process_obj.read()
    process_obj.process()
    process_obj.write()
    
  }
}