import java.io.IOException;

import org.apache.pig.*;
import org.apache.pig.data.Tuple;

public class filterQuality extends FilterFunc{

	@Override
	public Boolean exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		try{
			if(input==null) return false;
			if(input.size()==0) return false;
			if(input.get(0)==null) return false;
			if((int)input.get(0)==0 || (int)input.get(0)==1) return true;
			return false;
		}catch(Exception e){
			throw new IOException("quality filter failed",e);
		}
		
	}
}
