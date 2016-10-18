import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class countError extends EvalFunc<Double>  {
	
	@Override
	public Double exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		try{
			double sum = 0;
			double hit = 0;
			double error = 0;
			DataBag bag = (DataBag) input.get(0);
			Iterator itr = bag.iterator();
			while(itr.hasNext()){
				Tuple t = (Tuple) itr.next();
				if(t!=null && t.size()>0 && t.get(0)!=null){
					sum++;
					if(t.get(0).equals("Hit")) hit++;
					if(t.get(0).equals("Error")) error++;
				}
			}
			if(sum==0) return 0.0;
			return (error/sum);
		}catch(Exception e){
			throw new IOException("count error failed",e);
		}
		
	}
}
