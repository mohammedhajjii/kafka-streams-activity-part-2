package md.hajji;

public class Test {

    public static void main(String[] args) {


        try{
            throw new RuntimeException("execption throwed in try block");
        }catch (Exception exception){
            exception.printStackTrace();
        }
       finally {
            System.out.println("executed finally");
        }
        System.out.println("after all blocks");

    }
}
