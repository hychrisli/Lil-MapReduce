import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

    public String uri;
    public Integer cnt;

    public Pair() {
    }

    public Pair(String uri, Integer cnt) {
	this.uri = uri;
	this.cnt = cnt;
    }

    public void readFields(DataInput in) throws IOException {
	this.uri = in.readUTF();
	this.cnt = in.readInt();

    }

    public void write(DataOutput out) throws IOException {
	out.writeUTF(uri);
	out.writeInt(cnt);
    }

    public int compareTo(Pair that) {

	int cmp = compareInt(this.cnt, that.cnt);
	if (cmp != 0)
	    return cmp;
	return this.uri.compareTo(that.uri);
    }

    private int compareInt(int a, int b) {
	return (a < b ? -1 : (a == b ? 0 : 1));
    }

    @Override
    public String toString() {
	return uri + "\t" + cnt;
    }

}