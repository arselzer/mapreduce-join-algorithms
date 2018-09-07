import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface Join {
    public void init(JoinConfig config) throws IOException;
    public Job getJob();
}
