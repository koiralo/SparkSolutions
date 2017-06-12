import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by sakoirala on 6/9/17.
  */
object hdfsTests extends App{

  val conf = new Configuration()

  val path = new Path("hdfs://localhost:9000/tmp/")

  val fileSysTem =  FileSystem.get(conf)

  fileSysTem.listStatus(path).foreach(println)



}
