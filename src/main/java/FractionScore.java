import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Random;

class Spatial_Feature implements Serializable{
    private String feature_name;

    public Spatial_Feature(String feature_name) {
        this.feature_name = feature_name;
    }

    public String getFeature_name() {
        return feature_name;
    }

    public void setFeature_name(String feature_name) {
        this.feature_name = feature_name;
    }

}
class Spatial_Point implements Serializable {
    private Spatial_Feature feature_type;
    private int x,y;
//    private String name;

    public Spatial_Point(Spatial_Feature feature_type, int x, int y) {
        this.feature_type = feature_type;
        this.x = x;
        this.y = y;
    }
    public Spatial_Point( int x, int y) {
        this.x = x;
        this.y = y;
    }
    public Spatial_Feature getFeature_type() {
        return feature_type;
    }

    public void setFeature_type(Spatial_Feature feature_type) {
        this.feature_type = feature_type;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
}
public class FractionScore {

    public static void main(String args[])
    {
        Random random=new Random();
        // adding point to the list
        LinkedList<Spatial_Point> points_lists=new LinkedList<Spatial_Point>();//list of all the points
        for(int i=0;i<20;i++)
        {
            int x=random.nextInt(100);
            int y=random.nextInt(100);
            points_lists.add(new Spatial_Point(x,y));
        }
        String[] arr={"A","B","C","D"};
        LinkedList<Spatial_Feature> spatial_features=new LinkedList<Spatial_Feature>();//set of all spatial features
        for(int i=0;i<arr.length;i++)
        {
            Spatial_Feature f=new Spatial_Feature(arr[i]);
            spatial_features.add(f);
        }

        for(int i=0;i<points_lists.size();i++)
        {
            int ind=random.nextInt(spatial_features.size());

            points_lists.get(i).setFeature_type(spatial_features.get(ind));
            System.out.println(points_lists.get(i).getFeature_type().getFeature_name()+" "+points_lists.get(i).getX()+" "+points_lists.get(i).getY());
        }
        SparkSession spark= SparkSession.builder()
                .master("local")
                .appName("BDAProject")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        JavaRDD<Spatial_Point> points_rdd=sc.parallelize(points_lists);
        for(Spatial_Point person : points_rdd.collect()){
            System.out.println(person.getFeature_type().getFeature_name()+" "+person.getX()+" "+person.getY());
        }

    }
}
