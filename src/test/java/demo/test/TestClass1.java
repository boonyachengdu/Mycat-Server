package demo.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.PreparedStatement;

/**
 * @author mycat
 *
 */
public class TestClass1 {

	public static void main(String args[]) throws SQLException,
			ClassNotFoundException {
		String jdbcdriver = "com.mysql.jdbc.Driver";
		String jdbcurl = "jdbc:mysql://192.168.178.128:8066/TESTDB?useUnicode=true&characterEncoding=utf-8";
		String username = "root";
		String password = "123456";
		System.out.println("开始连接mysql:" + jdbcurl);
		Class.forName(jdbcdriver);
		Connection c = DriverManager.getConnection(jdbcurl, username, password);
		Statement st = c.createStatement();
		print("test jdbc ",	st.executeQuery("select count(*) from travelrecord "));
		
		System.out.println("OK......");
	}

	static void print(String name, ResultSet res) throws SQLException {
		System.out.println(name);
		ResultSetMetaData meta = res.getMetaData();
		// System.out.println( "\t"+res.getRow()+"条记录");
		String str = "";
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			str += meta.getColumnName(i) + "   ";
			// System.out.println( meta.getColumnName(i)+"   ");
		}
		System.out.println("\t" + str);
		str = "";
		while (res.next()) {
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				str += res.getString(i) + "   ";
			}
			System.out.println("\t" + str);
			str = "";
		}
	}
	
	/**
	 * 
	 * @param conn
	 * @param num
	 * @param initValue
	 * @throws SQLException
	 */
	static void insert(Connection conn,int num,long initValue) throws SQLException{ 
		/*insert(c, 100000, 1011104);*/
		//100 144ms
		//1000 315ms
		//10000 722ms
		//100000 13573 ms
		conn.setAutoCommit(false);// 即手动提交
		StringBuilder sql=new StringBuilder("insert into travelrecord(id) values ");
		int index=0;
		for (int i = 0; i < num; i++) {
			long result=initValue+i;
			if(index==num-1){
				sql.append("("+result+");");
			}else{
				sql.append("("+result+"),");
			}
			index++;
		}
		Long startTime = System.currentTimeMillis();
		System.out.println(sql.toString()); 
		PreparedStatement pstm = (PreparedStatement) conn.prepareStatement(sql.toString());
		pstm.execute();
		conn.commit();
		Long endTime = System.currentTimeMillis();
		
		String timeStr=(endTime - startTime)+" ms"; 
		System.out.println("OK,用时：" + timeStr);
	}
}
