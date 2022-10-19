package Item_Customer_Stripe;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class StripeValue extends MapWritable {

	public StripeValue() {
		super();
	}

	@Override
	public String toString() {
		String output = "[";
		for (Entry<Writable, Writable> entry : this.entrySet()) {
			output += String.format("(%s, %s), ", entry.getKey().toString(), entry
					.getValue().toString());
		}
		return output + "]";
	}

}
