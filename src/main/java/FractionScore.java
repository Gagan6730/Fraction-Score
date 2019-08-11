class Spatial_Feature {
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
class Spatial_Point
{
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
        
    }
}
