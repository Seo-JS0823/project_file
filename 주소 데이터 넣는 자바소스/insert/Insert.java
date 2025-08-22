package insert_data.insert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import insert_data.connection.Access;

public class Insert {

	private Access access = new Access();
	
	BlockingQueue<String> address = new LinkedBlockingQueue<>();
	
	public static void main(String[] args) throws Exception {
		Insert in = new Insert();
		// 완료 in.roadCodeParseFileWriter();
		// 완료 in.jibunParseFileWriter();
		// tlqkf 완료 in.roadSubDataParseFileWriter();
		// 완료 ㅎㅎ in.roadAddressParseFileWriter();
		
		in.insert2();
		
	}
	
	public void insert2() throws Exception {
		/* 아 욕나오네 */
		ExecutorService execute = Executors.newFixedThreadPool(10);
		
		Runnable worker = () -> {
			Connection con = null;
			PreparedStatement ps = null;
			
			try {
				con = access.getConnection();
				con.setAutoCommit(false);
				
				/*
				road_code
				INSERT INTO road_code (road_code, road_name, town_code, province, district, town)
				VALUES (?, ?, ?, ?, ?, ?)
				완료
				
				road_address
				INSERT INTO road_address (address_code, road_code, town_code, building_num_main, building_num_sub)
				VALUES (?, ?, ?, ?, ?)
				완료
				
				road_sub_data
				INSERT INTO road_subdata (address_code, zipcode, building_name)
				VALUES (?, ?, ?)
				완료
				
				jibun_address
				INSERT INTO jibun_address (address_code, province, district, town, lot_main, lot_sub)
				VALUES (?, ?, ?, ?, ?, ?)
				
				 */
				
				
				ps = con.prepareStatement("INSERT INTO jibun_address (address_code, province, district, town, lot_main, lot_sub)\r\n"
						+ "				VALUES (?, ?, ?, ?, ?, ?)");
				
				int batchSize = 0;
				
				while(true) {
					String readLine = address.take();
					
					if("finish".equals(readLine)) {
						break;
					}
					
					String[] parse = readLine.split(",", -1);
					
					String addressCode = parse[0];
					String roadCode = parse[1];
					String townCode = parse[2];
					String dada = parse[3];
					int lotMain = Integer.parseInt(parse[4]);
					int lotSub = Integer.parseInt(parse[5]);
					
					ps.setString(1, addressCode);
					ps.setString(2, roadCode);
					ps.setString(3, townCode);
					ps.setString(4, dada);
					ps.setInt(5, lotMain);
					ps.setInt(6, lotSub);
					
					ps.addBatch();
					batchSize++;
					
					
					if(batchSize % 2000 == 0) {
						ps.executeBatch();
						con.commit();
						batchSize = 0;
					}
				}
				ps.executeBatch();
				con.commit();
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				try {
					ps.close();
					con.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		for(int i = 0; i < 10; i++) {
			execute.submit(worker);
		}
		
		try(BufferedReader br = new BufferedReader(new FileReader("src/jibun_address.txt"))) {
			String line;
			while((line = br.readLine()) != null) {
				address.put(line);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		try {
			for(int i = 0; i < 10; i++) {
				address.put("finish");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		execute.shutdown();
		execute.awaitTermination(2, TimeUnit.HOURS);
	}
	
	public void roadAddressParseFileWriter() throws Exception {
		File file = new File("src/주소_충청북도.txt");
		int i = 0;
		int exp = 0;
		try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS949"))) {
			String line = "";
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("road_address.txt", true), "UTF-8"));
			while((line = br.readLine()) != null) {
				try {
					String[] parse = line.split("\\|", -1);
					String code = parse[0];
					if(parse.length < 10) {
						System.out.println(parse.length);
						System.out.println("에러라인 코드 : " + code);
						exp++;						
					}
					
					String roadCode = parse[1];
					String townCode = parse[2];
					String buildingNumMain = parse[4];
					String buildingNumSub = parse[5];
					
					bw.write(code + "," + roadCode + "," + townCode + "," + buildingNumMain + "," + buildingNumSub + "\n");
					i ++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			bw.flush();
			bw.close();
			System.out.println("에러 : " + exp + " 건");
			System.out.println("전체 : " + i + " 건");
		}
	}
	
	public void roadSubDataParseFileWriter() throws Exception {
		File file = new File("src/부가정보_충청북도.txt");
		int i = 0;
		int exp = 0;
		try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS949"))) {
			String line = "";
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("road_sub_data.txt", true), "UTF-8"));
			while((line = br.readLine()) != null) {
				try {
					String[] parse = line.split("\\|", -1);
					String code = parse[0];
					if(parse.length < 8) {
						System.out.println(parse.length);
						System.out.println("에러라인 코드 : " + code);
						exp++;						
					}
					
					String zip = parse[3];
					String build = parse[6];
					
					
					bw.write(code + "," + zip + "," + build + "\n");
					i ++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			bw.flush();
			bw.close();
			System.out.println("에러 : " + exp + " 건");
			System.out.println("전체 : " + i + " 건");
		}
	}
	
	public void jibunParseFileWriter() throws Exception {
		File file = new File("src/지번_충청북도.txt");
		
		try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS949"))) {
			String line = "";
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("jibun_address.txt", true), "UTF-8"));
			while((line = br.readLine()) != null) {
				try {
					/* index : */
					String[] parse = line.split("\\|");
					
					String postCode = parse[0];
					String province = parse[3];
					String district = parse[4];
					String town = parse[5];
					String lotMain = parse[8];
					String lotSub = parse[9];
					
					bw.write(postCode + "," + province + "," + district + "," + town + "," + lotMain + "," + lotSub + "\n");
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			bw.flush();
			bw.close();
		}
	}
	
	public void roadCodeParseFileWriter() throws Exception {
		File file = new File("src/개선_도로명코드_전체분.txt");
		
		try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS949"))) {
			String line = "";
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("road_code.txt", true), "UTF-8"));
			while((line = br.readLine()) != null) {
				try {
					/* index : */
					String[] parse = line.split("\\|");
					
					String code = parse[0].trim();
					String name = parse[1].trim();
					String townCode = parse[3].trim();
					String province = parse[4].trim();
					String district = parse[6].trim();
					String town = parse[8].trim();
					
					bw.write(code + "," + name + "," + townCode + "," + province + "," + district + "," + town + "\n");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			bw.flush();
			bw.close();
		}
	}
	
	
	public void insert() throws InterruptedException {
		
		/* 아 욕나오네 */
		ExecutorService execute = Executors.newFixedThreadPool(10);
		
		Runnable worker = () -> {
			Connection con = null;
			PreparedStatement ps = null;
			
			try {
				con = access.getConnection();
				con.setAutoCommit(false);
				
				ps = con.prepareStatement("insert into build_address(\r\n"
						+ "province,\r\n"
						+ "district,\r\n"
						+ "town,\r\n"
						+ "lot_main,\r\n"
						+ "lot_sub,\r\n"
						+ "road_name,\r\n"
						+ "build_num_main,\r\n"
						+ "build_num_sub,\r\n"
						+ "building_name,\r\n"
						+ "detailed_building,\r\n"
						+ "zipcode,\r\n"
						+ "row_num)\r\n"
						+ "Values\r\n"
						+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, build_address_rownum.NEXTVAL)");
				
				int batchSize = 0;
				
				while(true) {
					String readLine = address.take();
					
					if("finish".equals(readLine)) {
						break;
					}
					
					/*
					 1 시도명         : province
					 2 시군구명       : district
					 3 법정읍면동명   : town
					 4 지번본번       : lotMain          int
					 5 지번부번       : lotSub           int
					 6 도로명         : roadName
					 7 건물본번       : buildNumMain     int
					 8 건물부번       : buildNumSub      int
					 9 건물명         : buildingName
					 10 상세건물명    : detailedBuilding
					 11 우편번호      : zipcode
					 */
					String[] datas = readLine.split(",");
					String province = datas[0];
					String district = datas[1];
					String town = datas[2];
					int lotMain = Integer.parseInt(datas[3]);
					int lotSub = Integer.parseInt(datas[4]);
					String roadName = datas[5];
					int buildNumMain = Integer.parseInt(datas[6]);
					int buildNumSub = Integer.parseInt(datas[7]);
					String buildingName = datas[8];
					String detailedBuilding = datas[9];
					String zipcode = datas[10];
					
					ps.setString(1, province);
					ps.setString(2, district);
					ps.setString(3, town);
					ps.setInt(4, lotMain);
					ps.setInt(5, lotSub);
					ps.setString(6, roadName);
					ps.setInt(7, buildNumMain);
					ps.setInt(8, buildNumSub);
					ps.setString(9, buildingName);
					ps.setString(10, detailedBuilding);
					ps.setString(11, zipcode);
					
					ps.addBatch();
					batchSize++;
					
					System.out.println(province + district + roadName);
					
					if(batchSize % 2000 == 0) {
						ps.executeBatch();
						con.commit();
						batchSize = 0;
					}
				}
				ps.executeBatch();
				con.commit();
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				try {
					ps.close();
					con.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		for(int i = 0; i < 10; i++) {
			execute.submit(worker);
		}
		
		try(BufferedReader br = new BufferedReader(new FileReader("src/build_address.txt"))) {
			String line;
			while((line = br.readLine()) != null) {
				address.put(line);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		try {
			for(int i = 0; i < 10; i++) {
				address.put("finish");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		execute.shutdown();
		execute.awaitTermination(2, TimeUnit.HOURS);
		
	}
}
