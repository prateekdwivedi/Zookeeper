package DataMonitor;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistCalcMaster implements Watcher{
	private static final int SESSION_TIMEOUT=5000;
	private ZooKeeper zk;
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
	
     public void create(String groupName, String data,int check)throws KeeperException,InterruptedException{
			String path ="/"+groupName;
			String createPath = null;
			
    // We are creating a ZNode without any data
			if(check ==0){
			if(data == null){
				createPath= zk.create(path,null/*data*/,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			else{
				 createPath= zk.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
			}
			}
			if(check==1){
			createPath= zk.create(path,null/*data*/,Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
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
					createGroup.create(znodes[i],null,0);
				}
				else{
					System.out.println("Znode exists");
				}
		   }
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		   
	   }
	   
	 
	 // Method to close the object of ZooKeeper
	   public void close() throws InterruptedException{
			zk.close();
		}
	   
	   // Creating the object of CreateGroup 
	   static DistCalcMaster createGroup = new DistCalcMaster();
	        
	   
	   // Main Function
	   
	   public static void main (String[] args )throws Exception{
		// createGroup.create("/queue",null,1);		//creating a Znode to store queue
		 String[] znodes = new String[4];
		 znodes[0] = args[1];
		 znodes[1] = args[3];
		 znodes[2] = args[5];
		 znodes[3] = "results";
		createGroup.connect("127.0.0.1");
		createGroup.createBaseZnodes(znodes);  //creates base znodes if it does not exist
		
		// sdtin <Operation-Name>@<Operation>
		
		Scanner scan = new Scanner(System.in);
		String command = null;
		
		// To perform the exit operation
		while(command != "exit"){
			command = scan.nextLine();
			if(command.equals("exit")){
				System.exit(0);
				break;
			}
			
			
		// To perform the Display operation of Specified ZNode	
			String [] catCommand = command.split(" ");
			
			if(catCommand[0].equals("cat")){
				try{
				byte[] data = createGroup.read(catCommand[1]);
				for(byte b:data){
					System.out.print((char) b);
				 } 
				}
				catch(NoNodeException e)
				{
					System.err.println(catCommand[1] + " is not found");
				}
			 continue;
			}
		
			
		// To send the request to create a znode using <Operation-Name>@<Operation>
			
		String [] commands = command.split("@");
		String path= "/"+znodes[0]+"/"+commands[0];
		
		String data = commands[1];
		
		Stat stat = createGroup.znode_exists(path); // Stat checks the path of the znode
		try{
        if(stat != null) {
           System.out.println("Enter a different operation name");
        } else {
          
           path = path.substring(1, path.length());
           
           createGroup.create(path,data,0); //normal call
          
        }
		}
		catch(Exception e){
			System.out.println(e.getMessage());
			
		}
	}
		
		createGroup.close();
		
	}
	
	

}
