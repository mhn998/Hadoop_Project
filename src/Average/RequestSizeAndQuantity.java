import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

class RequestSizeAndQuantity extends ObjectWritable implements WritableComparable<RequestSizeAndQuantity> {
    private IntWritable requestSize;
    private IntWritable count;

    public RequestSizeAndQuantity() {
        this.requestSize = new IntWritable(0);
        this.count = new IntWritable(0);
    }

    public RequestSizeAndQuantity(IntWritable requestSize,IntWritable count) {
        super(ObjectWritable.class);

        this.requestSize = requestSize;

        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        requestSize.write(dataOutput);
        count.write(dataOutput);

    }
    public void readFields(DataInput dataInput) throws IOException {
        requestSize.readFields(dataInput);
        count.readFields(dataInput);

    }

    public void setRequestSize(IntWritable requestSize)
    {
        this.requestSize = requestSize;
    }

    public void setCount(IntWritable count)
    {
        this.count = count;
    }

    public IntWritable getRequestSize() {
        return this.requestSize;
    }
    public IntWritable getCount() {
        return this.count;
    }

    @Override
    public int compareTo(RequestSizeAndQuantity o) {
        // TODO Auto-generated method stub
        return Integer.compare(this.requestSize.get(), o.getRequestSize().get());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestSizeAndQuantity that = (RequestSizeAndQuantity) o;
        return Objects.equals(requestSize, that.requestSize);
    }
    @Override
    public int hashCode() {
        return Objects.hash(requestSize);
    }

//    @Override
//    public ObjectWritable get() {
//        return (RequestSizeAndQuantity) super.get();
//    }
//
//
//    @Override
//    public String toString() {
//    	ObjectWritable values = get();
//        return values.get().toString() ;
//    }


}
