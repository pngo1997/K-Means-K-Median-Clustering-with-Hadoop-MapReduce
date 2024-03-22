import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.io.IOException;

public class MapReduceChain extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        JobControl jobControl = new JobControl("Kmedian");

        String[] job1Args = new String[]{
            "-mapper", "kmedian_mapper.py",
            "-reducer", "kmedian_reducer.py",
            "-input", "/data/kmeans_data.csv",
            "-output", "/Part1_Output1_Median/",
            "-file", "kmedian_mapper.py",
            "-file", "kmedian_reducer.py",
            "-file", "centers.txt"
        };
        JobConf job1Conf = StreamJob.createJob(job1Args);
        Job job1 = new Job(job1Conf);
        jobControl.addJob(job1);

        if (JobClient.runJob(job1Conf).isSuccessful()) {
            String outputFirstJob = "/Part1_Output1_Median/part-00000";
            String localCentersFile = "centers1.txt";
            String[] copyToLocalArgs = {"-copyToLocal", outputFirstJob, localCentersFile};
            ToolRunner.run(new Configuration(), new org.apache.hadoop.fs.FsShell(), copyToLocalArgs);

            String previousCentersFile = "centers.txt";
            String newCentersDir = "centersMedian_Chain";

            try {
                File centersMedianDir = new File(newCentersDir);
                centersMedianDir.mkdir();

                File originalCenters = new File(previousCentersFile);
                Files.move(originalCenters.toPath(), new File(centersMedianDir, previousCentersFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String[] job2Args = new String[]{
            "-mapper", "kmedian_mapper.py",
            "-reducer", "kmedian_reducer.py",
            "-input", "/data/kmeans_data.csv",
            "-output", "/Part1_Output2_Median/",
            "-file", "kmedian_mapper.py",
            "-file", "kmedian_reducer.py",
            "-file", "centers1.txt"
        };
        JobConf job2Conf = StreamJob.createJob(job2Args);
        Job job2 = new Job(job2Conf);
        jobControl.addJob(job2);

	if (JobClient.runJob(job2Conf).isSuccessful()) {
            String outputFirstJob = "/Part1_Output2_Median/part-00000";
            String localCentersFile = "centers2.txt";
            String[] copyToLocalArgs = {"-copyToLocal", outputFirstJob, localCentersFile};
            ToolRunner.run(new Configuration(), new org.apache.hadoop.fs.FsShell(), copyToLocalArgs);

            String previousCentersFile = "centers1.txt";
	    String existingCentersDir = "centersMedian_Chain";

            try {
                File originalCenters = new File(previousCentersFile);
                Files.move(originalCenters.toPath(), new File(existingCentersDir, previousCentersFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

	String[] job3Args = new String[]{
            "-mapper", "kmedian_mapper.py",
            "-reducer", "kmedian_reducer.py",
            "-input", "/data/kmeans_data.csv",
            "-output", "/Part1_Output3_Median/",
            "-file", "kmedian_mapper.py",
            "-file", "kmedian_reducer.py",
            "-file", "centers2.txt"
        };
        JobConf job3Conf = StreamJob.createJob(job3Args);
        Job job3 = new Job(job3Conf);
        jobControl.addJob(job3);

	if (JobClient.runJob(job3Conf).isSuccessful()) {
            String outputFirstJob = "/Part1_Output3_Median/part-00000";
            String localCentersFile = "centers3.txt";
            String[] copyToLocalArgs = {"-copyToLocal", outputFirstJob, localCentersFile};
            ToolRunner.run(new Configuration(), new org.apache.hadoop.fs.FsShell(), copyToLocalArgs);

            String previousCentersFile = "centers2.txt";
	    String existingCentersDir = "centersMedian_Chain";

            try {
                File originalCenters = new File(previousCentersFile);
                Files.move(originalCenters.toPath(), new File(existingCentersDir, previousCentersFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

	String[] job4Args = new String[]{
            "-mapper", "kmedian_mapper.py",
            "-reducer", "kmedian_reducer.py",
            "-input", "/data/kmeans_data.csv",
            "-output", "/Part1_Output4_Median/",
            "-file", "kmedian_mapper.py",
            "-file", "kmedian_reducer.py",
            "-file", "centers3.txt"
        };
        JobConf job4Conf = StreamJob.createJob(job4Args);
        Job job4 = new Job(job4Conf);
        jobControl.addJob(job4);

	if (JobClient.runJob(job4Conf).isSuccessful()) {
            String outputFirstJob = "/Part1_Output4_Median/part-00000";
            String localCentersFile = "centers4.txt";
            String[] copyToLocalArgs = {"-copyToLocal", outputFirstJob, localCentersFile};
            ToolRunner.run(new Configuration(), new org.apache.hadoop.fs.FsShell(), copyToLocalArgs);

            String previousCentersFile = "centers3.txt";
    	    String existingCentersDir = "centersMedian_Chain";

            try {
                File originalCenters = new File(previousCentersFile);
                Files.move(originalCenters.toPath(), new File(existingCentersDir, previousCentersFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Thread runJobControl = new Thread(jobControl);
        runJobControl.start();
        while (!jobControl.allFinished()) {
            // wait here
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new MapReduceChain(), args);
        System.exit(result);
    }
}
