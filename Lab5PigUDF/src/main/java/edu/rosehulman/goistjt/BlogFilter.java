package edu.rosehulman.goistjt;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlogFilter extends FilterFunc {
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<>();
        funcSpecs.add(new FuncSpec(this.getClass().getName(),
                new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcSpecs;
    }


    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) {
            return null;
        }
        Object o = input.get(0);
        if(o==null){
            return null;
        }

        String csUriStem = (String) o;
        Pattern pattern = Pattern.compile("/blogs/.*?/");
        Matcher matcher = pattern.matcher(csUriStem);
        return matcher.find();
    }
}
