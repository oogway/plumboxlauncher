package in.oogway.plumbox.launcher.views;

import java.io.Serializable;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 07/04/18.
 */
public class ViewBean implements Serializable {
    private String name;
    private Integer rollNo;

    public Integer getRollNo() {
        return rollNo;
    }

    public void setRollNo(Integer rollNo) {
        this.rollNo = rollNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
