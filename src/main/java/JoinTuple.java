import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class JoinTuple implements WritableComparable<JoinTuple> {
    public IntWritable tableIndex;
    public Text tuple;

    public JoinTuple(int i, Text tuple) {
        tableIndex = new IntWritable(i);
        this.tuple = tuple;
    }

    public IntWritable getTableIndex() {
        return tableIndex;
    }

    public void setTableIndex(IntWritable tableIndex) {
        this.tableIndex = tableIndex;
    }

    public Text getTuple() {
        return tuple;
    }

    public void setTuple(Text tuple) {
        this.tuple = tuple;
    }

    public int compareTo(JoinTuple joinTuple) {
        return tuple.compareTo(joinTuple.tuple);
    }

    public void write(DataOutput dataOutput) throws IOException {
        tableIndex.write(dataOutput);
        tuple.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        tableIndex.readFields(dataInput);
        tuple.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinTuple joinTuple = (JoinTuple) o;
        return Objects.equals(tableIndex, joinTuple.tableIndex) &&
                Objects.equals(tuple, joinTuple.tuple);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tableIndex, tuple);
    }

    @Override
    public String toString() {
        return "JoinTuple{" +
                "tableIndex=" + tableIndex +
                ", tuple=" + tuple +
                '}';
    }
}
