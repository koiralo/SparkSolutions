import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sakoirala on 6/9/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Path path = new Path("hdfs://localhost:9000/tmp");

        FileSystem fs = FileSystem.get(path.toUri(), conf);

        FileStatus[] fileStatus = fs.listStatus(path);

        List<FileStatus> filesToDelete = new ArrayList<FileStatus>();

        for (FileStatus file: fileStatus) {

            if (file.getPath().getName().startsWith(".hive-staging_hive")){
                filesToDelete.add(file);
            }
        }


        for (int i=0; i<filesToDelete.size();i++){
            fs.delete(filesToDelete.get(i).getPath(), true);
        }

    }
}
