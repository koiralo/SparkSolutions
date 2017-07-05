import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

/**
  * Created by sakoirala on 6/9/17.
  */
object hdfsTests extends App{

/*  val conf = new Configuration()

  val path = new Path("hdfs://localhost:9000/tmp/")

  val fileSysTem =  FileSystem.get(path.toUri, conf)

  fileSysTem.listStatus(path).foreach(println)*/

  val data = Source.fromFile("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/layout.txt")

  val script = "/home/sakoirala/panew/DasEngine/DasProduction/jobs/ReorderJobNumsCascading.py"

  data.getLines().foreach( line => {
    Runtime.getRuntime.exec(s"python3 ${script} ${line}")
  })

}
