import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.streaming.StreamJob;


public class MapReduceChain extends Configured implements Tool
{
    public int run( String[] args) throws Exception
    {
        JobControl jobControl = new JobControl( "Part2");

        String[] job1Args = new String[]
        {
            "-mapper"   , "part2_mapper1.py",
            "-reducer"  , "part2_reducer1.py",
            "-input"    , "/data/lineorder.tbl",
            "-input"    , "/data/part.tbl",
            "-output"   , "/Part2_MR1_Output/",
            "-file"	    , "part2_mapper1.py",
            "-file"	    , "part2_reducer1.py"
        };
        JobConf job1Conf = StreamJob.createJob(job1Args);
        Job job1 = new Job(job1Conf);
        jobControl.addJob(job1);

        String[] job2Args = new String[]
        {
            "-mapper"   , "part2_mapper2.py",
            "-reducer"  , "part2_reducer2.py",
            "-input"    , "/Part2_MR1_Output/part-00000",
            "-output"   , "/Part2_MR2_Output/",
            "-file"     , "part2_mapper2.py",
            "-file"     , "part2_reducer2.py",
            "-file"     , "dwdate.tbl"
        };
        JobConf job2Conf = StreamJob.createJob(job2Args);
        Job job2 = new Job(job2Conf);
        job2.addDependingJob(job1);
        jobControl.addJob(job2);

        Thread runJobControl = new Thread(jobControl);
        runJobControl.start();
        while(!jobControl.allFinished())
        {
            // wait here
        }

        return 0;
    }

    public static void main( String[] args) throws Exception
    {
        int result = ToolRunner.run(new Configuration(), new MapReduceChain(), args);
        System.exit(result);
    }
}