import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.VALUESET;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
 
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ComputeMax extends EvalFunc <Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		
		Iterator<Object>itr = input.iterator();
		double result = Double.NEGATIVE_INFINITY;
		while(itr.hasNext()){
			double value = (double) itr.next();
			result = Math.max(result, value);
		}
		return result;
	}

	
}
