package in.oogway.plumbox.launcher.views;

import java.util.HashMap;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 02/04/18.
 *
 * This is an implementation of the RedShift viewer to create a view in RedShift Spectrum
 *
 */


public class RedShiftViews implements RedShiftViewer {

    @Override
    public String destinationView() {
        return "sample_table";
    }

    @Override
    public String destinationWarmView() {
        return null;
    }

    @Override
    public HashMap<String, Integer> getViewsAndPartitionNumber() {
        HashMap<String, Integer> hm=new HashMap<String,Integer>();
        hm.put(destinationView(), 30);

        return hm;
    }

    @Override
    public String getPartitionColumn() {
        return "date";
    }

    @Override
    public String storageFormat() {
        return "parquet";
    }

    @Override
    public String getSchemaName() {
        return "spectrum";
    }

    @Override
    public String partitionColumnDataType() {
        return "TIMESTAMP";
    }

}
