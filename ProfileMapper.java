import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class ProfileMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text columnName = new Text();

    private List<String> columnHeaders;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        
	    columnHeaders = Arrays.asList(
                "INCIDENT_KEY", "OCCUR_DATE", "OCCUR_TIME", "BORO", "LOC_OF_OCCUR_DESC",
                "PRECINCT", "JURISDICTION_CODE", "LOC_CLASSFCTN_DESC", "LOCATION_DESC",
                "STATISTICAL_MURDER_FLAG", "PERP_AGE_GROUP", "PERP_SEX", "PERP_RACE",
                "VIC_AGE_GROUP", "VIC_SEX", "VIC_RACE", "X_COORD_CD", "Y_COORD_CD",
                "Latitude", "Longitude", "Lon_Lat"
        );
    }
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",", -1);

        if (columns[0].equals("INCIDENT_KEY")) {
            return;

	}
        
        if (columns.length != columnHeaders.size()) {
            return;
	}
	for (int i = 0; i < columnHeaders.size(); i++) {
            columnName.set(columnHeaders.get(i));

            String columnValue = (i < columns.length) ? columns[i] : "";

            if (columnValue.isEmpty()) {
                columnName.set(columnHeaders.get(i) + "_EMPTY");
            } else {
                columnName.set(columnHeaders.get(i));
            }
            context.write(columnName, one);
        }
    }

}
