package DataMonitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
public class DistCalcSlave implements Watcher{
	private static final int SESSION_TIMEOUT=5000;
	private static ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
// Connect method for Zookeeper	
	public void connect(String hosts)throws IOException, InterruptedException{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT,this);
		connectedSignal.await();
	}
	
	@Override
	public void process(WatchedEvent event) {
		if(event.getState()== KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
		
	}
	

    // Method to create a ZNode it takes two argument
	
     public void create(String groupName, String data)throws KeeperException,InterruptedException{
       	   
			String path ="/"+groupName;
			
			String createPath = null;
			
    // We are creating a ZNode without any data
		
			if(data == null){
				createPath= zk.create(path,null/*data*/,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			else{
				 createPath= zk.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
			}
			
			
			System.out.println("Created "+createPath);
		}
	
	
	
	// Method to check existence of znode and its st,tus, if znode is available.
	   public  Stat znode_exists(String path) throws
	      KeeperException,InterruptedException {
	      return zk.exists(path, true);   
	   }
	   
    
	// Method to get data of Specified Znode
		public byte[] read(String path) throws KeeperException,InterruptedException {
			return zk.getData(path,true,zk.exists(path,true));
		}
	   
		
    // As per specification we are creating ZNode through command line argument		
	   public void createBaseZnodes(String[] znodes){
		    
		   try {
		    for(int i =0;i<znodes.length;i++){
				Stat stat = createGroup.znode_exists("/"+znodes[i]);
				
				if(stat == null){
					createGroup.create(znodes[i],null);
				}
				else{
					System.out.println("Znode exists");
				}
		   }
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		   
	   }
	   
	 //method to delete a Znode
	 		public void delete(String path) throws KeeperException,InterruptedException {
	 		      zk.delete(path,zk.exists(path,true).getVersion());
	 		   }
	   
	 // Method to close the object of ZooKeeper
	   public void close() throws InterruptedException{
			zk.close();
		}
	   
	   
	   
	   // Method for getChildZnode
	   
	   public ArrayList <String> getChildZNode(String path) throws KeeperException, InterruptedException{
		   ArrayList <String> children = new ArrayList<String>();
		   try {
			   System.out.println(path);
		         Stat stat = createGroup.znode_exists(path); // Stat checks the path
		         if(stat!= null) {

		            //“getChildren” method- get all the children of znode.It has two args, path and watch
		        	 children=(ArrayList<String>) zk.getChildren(path, false);		           
		              } else {
		                System.out.println("Node does not exists");
		            }

		      } catch(NoNodeException e) {
		         System.out.println(e.getMessage());
		      }
		   return children;
	   }
	   
	   //performs calculations
	   public  String calculator(String str){
		    ScriptEngineManager mgr = new ScriptEngineManager();
		    ScriptEngine engine = mgr.getEngineByName("JavaScript");
		    String result = null;
			try {
				result = engine.eval(str).toString();
			} catch (ScriptException e) {
				e.printStackTrace();
			}
		    return result;
	   }
	   
	   
	   
	   // Creating the object of CreateGroup 
	   static DistCalcSlave createGroup = new DistCalcSlave();
	        
	   
	  
	   
	   // Main Function
	   
	   public static void main (String[] args )throws Exception{
		   
		// createGroup.create("/queue",null,1);		//creating a Znode to store queue
		   Scanner scan = new Scanner(System.in);
		 String[] znodes = new String[4];
		 znodes[0] = args[1];
		 znodes[1] = args[3];
		 znodes[2] = args[5];
		 znodes[3] = "results";
		createGroup.connect("127.0.0.1");
		createGroup.createBaseZnodes(znodes);  //creates base znodes if it does not exist
         
		List<String> znod = new ArrayList<String>();
		//getting list of znodes inside /requests
		try {
		znod = createGroup.getChildZNode("/"+znodes[0]);
		System.err.println("first value "+znod.get(0));
		} 
		catch(IndexOutOfBoundsException e){
			System.out.println("Sorry!! No Child node found in request");
		}
		for(int i = 0; i < znod.size(); i++)
        System.out.println(znod.get(i)); //Print children
		
		// if /request is not empty
		if(!znod.isEmpty()){
			
			String data = "";
			byte[] data1 = createGroup.read("/"+ znodes[0] +"/"+znod.get(0));
			for(byte b:data1){
				data = data + (char) b;
			 } 
			System.out.println(data + " <- Data");
			
			//delete znode from /request
			createGroup.delete("/"+ znodes[0] +"/"+znod.get(0));
		
			
			//create a  znode under processing
			System.out.println(" Proceesing path :"+ (znodes[2] +"/"+znod.get(0)));
			createGroup.create(znodes[2] +"/"+znod.get(0),data);
			
			
			//perform arithmetic/logical function here
			
			 String resultOfCalc = createGroup.calculator(data);
			System.err.println(resultOfCalc);
			Stat stat = createGroup.znode_exists("/"+znodes[3]+"/"+znod.get(0)); 
			 
			 	
			 
			//write result as child of znode
			 if(stat == null){ 
				 createGroup.create(znodes[3]+"/" +znod.get(0),resultOfCalc);
			 		}
				 else{ 
				 System.err.println("node already present inside znode");
				 }
			
			 //remove znode under processing
			 createGroup.delete("/"+ znodes[2] +"/"+znod.get(0));
			
			 
			
		}
	
		
		createGroup.close();
		
	}
	
	

}
