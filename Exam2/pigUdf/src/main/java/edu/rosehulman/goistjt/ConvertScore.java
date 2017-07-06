package edu.rosehulman.goistjt;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConvertScore extends EvalFunc<String> {

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<>();
        funcSpecs.add(new FuncSpec(this.getClass().getName(),
                new Schema(new Schema.FieldSchema(null, DataType.FLOAT))));
        return funcSpecs;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) return null;
        if (input.get(0) == null) return null;
        float grade = (float) input.get(0);
        return grade >= 90 ? "A" : grade > 80 ? "B" : grade > 70 ? "C" : grade > 60 ? "D" : "F";
    }

}
