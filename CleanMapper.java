import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class CleanMapper extends Mapper<Object, Text, NullWritable, Text> {

    private List<String> columnHeaders;
    private List<String> columnsToRemove;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        columnHeaders = Arrays.asList(
                "INCIDENT_KEY", "OCCUR_DATE", "OCCUR_TIME", "BORO", "LOC_OF_OCCUR_DESC",
                "PRECINCT", "JURISDICTION_CODE", "LOC_CLASSFCTN_DESC", "LOCATION_DESC",
                "STATISTICAL_MURDER_FLAG", "PERP_AGE_GROUP", "PERP_SEX", "PERP_RACE",
                "VIC_AGE_GROUP", "VIC_SEX", "VIC_RACE", "X_COORD_CD", "Y_COORD_CD",
                "Latitude", "Longitude", "Lon_Lat"
        );

        columnsToRemove = Arrays.asList(
                "LOCATION_DESC", "LOC_OF_OCCUR_DESC", "PERP_AGE_GROUP", "PERP_RACE",
                "LOC_CLASSFCTN_DESC", "PERP_SEX"
        );
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",", -1);

        if (columns.length < columnHeaders.size()) {
            System.err.println("This line has insufficient columns: " + line);
            return;
        }

        List<String> cleanedData = new ArrayList<>();
        for (int i = 0; i < columnHeaders.size(); i++) {
            String header = columnHeaders.get(i);

            if (!columnsToRemove.contains(header)) {
                String columnValue = (i < columns.length && !columns[i].isEmpty()) ? columns[i] : "UNKNOWN";
                cleanedData.add(columnValue);
            }
        }

        if (cleanedData.get(0).equals("UNKNOWN")) {
            return;
        }

        String cleanedLine = String.join(",", cleanedData);
        context.write(NullWritable.get(), new Text(cleanedLine));
    }
}

