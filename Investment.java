import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class InvestmentPortfolio {

	public static void main(String[] args) throws SQLException {

		Connection myConn = null;
		Statement myStmt = null;
		PreparedStatement myStmt1 = null;
		ResultSet myRs = null,myRs1 = null,myRs2 = null;
		
		try {
			// 1. Get a connection to database
			myConn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/demo?user=student&password=*****");
			
			System.out.println("Database connection successful!\n");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			//get the total amount value from the holding table.
			myRs1 = myStmt.executeQuery("select SUM(AMT) from HOLDING");
			int total = 0;
			if (myRs1.next() ) {
				  total = myRs1.getInt(1);
				}
			System.out.println(total);
			// 3. Execute SQL query
			myRs = myStmt.executeQuery("select * from HOLDING H INNER JOIN MODEL M WHERE H.SEC=M.SEC");
			
			// 4. Process the result set
			int i=0;
			float[] returnValues = new float[10];
			Map< String,Float> hm =  
                    new HashMap< String,Float>(); 
			String[] secArray = new String[10];
			//Calculation of whether it will be a BUY or SELL transaction.
			while (myRs.next()) {
				returnValues[i]= myRs.getFloat("AMT")-((myRs.getFloat("PERCENT")/100)*total);
				secArray[i] = myRs.getString("SEC");
				System.out.println(returnValues[i]);
				if(returnValues[i] >= 0) {
					hm.put("S",returnValues[i]);
				}
				else {
					hm.put("B",Math.abs(returnValues[i]));
				}
				i++;
			}
			int flag=0;
			//Persisting the result into ORD table.
	        for(Map.Entry entry: hm.entrySet()){
	        	System.out.println(secArray[flag]);
	            //String sql = "INSERT INTO ORD VALUES(?,?,?)";
	            myStmt1 = myConn.prepareStatement("INSERT INTO ORD VALUES(?,?,?)");
	            myStmt1.setString(1, secArray[flag]);
	            myStmt1.setString(2, (String) entry.getKey());
	            myStmt1.setFloat(3, (float) entry.getValue());
			 myStmt1.executeUpdate();
	        	flag++;
		}		
		//Verify this by getting a list of ORD
		myRs2 = myStmt1.executeQuery("select * from ORD");
		
		// Printing the rows from the ORD table.
		while (myRs2.next()) {
			System.out.println(myRs2.getString("SEC") + ", " + myRs2.getString("TRANS") + ", " + myRs2.getFloat("AMT"));
		}
				//Set entry=hm.entrySet();
			
			
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				myRs.close();
			}
			if (myRs1 != null) {
				myRs1.close();
			}
			if (myRs2 != null) {
				myRs2.close();
			}
			if (myStmt != null) {
				myStmt.close();
			}
			if (myStmt1 != null) {
				myStmt1.close();
			}
			
			if (myConn != null) {
				myConn.close();
			}
		}
	}

}
