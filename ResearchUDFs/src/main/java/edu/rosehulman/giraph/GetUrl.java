package edu.rosehulman.giraph;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetUrl extends EvalFunc<String> {
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
        funcSpecs.add(new FuncSpec(this.getClass().getName(),
                new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcSpecs;
    }

    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1)
            return null;

        String val = (String) input.get(0);
        while (val.matches("[{0-9}-].*")) {
            val = val.substring(1);
        }
        return val;
    }
}
