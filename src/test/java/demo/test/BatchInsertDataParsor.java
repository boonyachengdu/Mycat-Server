package demo.test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetMetaData;
/**
 * 测试数据分析类
 * 
 * @author pengjunlin
 *
 */
public class BatchInsertDataParsor {
	
	private String driver = "com.mysql.jdbc.Driver";

	private String url = "jdbc:mysql://192.168.178.128:8066/TESTDB?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true";//要5.1.13以上版本的驱动包

	private String user = "root";

	private String password = "123456";
	
	
	@Test
	public void queryData(){
		Connection conn = null;
		ResultSet rs=null;
		try {
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(url, user, password);
			String sql = "/*#mycat:db_type=slave*/SELECT id,pmethod,plimit,ptime,systime FROM processtask ;";
			long startTime=System.currentTimeMillis();
			rs=conn.prepareStatement(sql).executeQuery(sql);
			if(rs==null){
				throw new RuntimeException("ResultSet is null。。。。");
			}
			long endTime=System.currentTimeMillis();
			long cost=endTime-startTime;
			System.out.println("Totoal rows:"+rs.getRow()+" cost:"+cost+"ms");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			try {
				if(rs!=null&&!rs.isClosed()){
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	

	
	@Test
	public void parseTimeTest(){
		Connection conn = null;
		ResultSet rs=null;
		try {
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(url, user, password);
			String sql = "/*#mycat:db_type=slave*/SELECT avg(ptime) avg,max(ptime) max,min(ptime) min FROM processtask;";
			rs=conn.prepareStatement(sql).executeQuery(sql);
			if(rs==null){
				throw new RuntimeException("ResultSet is null。。。。");
			}
			ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();//获取键名
			int columnCount = md.getColumnCount();//获取行的数量
			while (rs.next()) {
			   for (int i = 1; i <= columnCount; i++) {
			     System.out.println(md.getColumnName(i)+": "+rs.getString(i));//获取键名及值
			   }
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			try {
				if(rs!=null&&!rs.isClosed()){
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	

	@Test
	public void parseLimitAndTimeTest(){
		Connection conn = null;
		ResultSet rs=null;
		try {
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(url, user, password);
			String sql = "/*#mycat:db_type=slave*/SELECT plimit,avg(ptime) avg FROM processtask group by plimit;";
			rs=conn.prepareStatement(sql).executeQuery(sql);
			if(rs==null){
				throw new RuntimeException("ResultSet is null。。。。");
			}
			ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();//获取键名
			int columnCount = md.getColumnCount();//获取字段的数量
			while (rs.next()) {
			   float limit=0,avg=0;
			   for (int i = 1; i <= columnCount; i++) {
				 float result=rs.getFloat(i);
			     //System.out.println(md.getColumnName(i)+": "+result+"");//获取键名及值
			     if(i==1){
			    	 limit=result;
			     }else{
			    	 avg=result;
			     }
			   }
			   System.out.println("limit="+limit+"\t\t估算1s钟大概的批量插入量:"+(1000*limit)/avg); 
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			try {
				if(rs!=null&&!rs.isClosed()){
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
