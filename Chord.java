import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

import javax.xml.bind.DatatypeConverter;

class fingerTable{
	
	InetAddress successorIp;
	int start,end,successorId,successorPort;
	
	public fingerTable(int start,int end,int successorId,int successorPort,InetAddress successorIp)
	{
		this.start  = start;
		this.end = end;
		this.successorIp = successorIp;
		this.successorId = successorId;
		this.successorPort = successorPort;
	}
	
	public void display()
	{
		System.out.println(start + " " + end + " " + successorId + " " + successorIp + " " + successorPort);
	}
	
}

class query extends Thread
{
	public void run()
	{
		while(true)
		{
		
			if(!Chord.isAlive)
			{
				return;
			}
			System.out.println("\n\n");
			System.out.println("Enter your choice");
			System.out.println("1. Node Id, Node Ip, Node Port");
			System.out.println("2. Node Successor");
			System.out.println("3. Node Predecessor");
			System.out.println("4. Finger Table");
			System.out.println("5. Files");
			System.out.println("6. Exit");
			System.out.println("\n\n");
			
			@SuppressWarnings("resource")
			Scanner scan  = new Scanner(System.in);
			int choice = scan.nextInt();
			switch(choice)
			{
				case 1: System.out.println("NodeID is " + Chord.nodeId + " NodeIp is " + Chord.nodeIp.getHostAddress() + " NodePort is " + Chord.nodePort);
						break;
				case 2: System.out.println("Successor ID is " + Chord.finger[0].successorId + " Successor Ip is " + Chord.finger[0].successorIp);
						break;
				case 3: System.out.println("Predecessor ID is " + Chord.predecessorId + " Predecessor Ip is " + Chord.predecessorIp);
						break;
						
				case 4:	for(int i=0;i<5;i++)
							Chord.finger[i].display();
						break;
						
				case 5: File f = new File("./files" + Integer.toString(Chord.nodeId));
						if(f.exists())
						{
							System.out.println("Files that node " + Chord.nodeId + " contains are");
							File[] filesInit = f.listFiles();
							for(File file : filesInit)
							{
								String filename = file.getName();
								int key;
								try {
									key = Chord.SHA1(filename.substring(0, filename.length()-3));
									System.out.println(key + " " + filename);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								
							}
						}
						else
							System.out.println("Node " + Chord.nodeId + " contains no files");
							
						break;
						
				case 6:	try {
						Chord.handleExit();
						return;
						}
						catch (IOException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
							return;
						}
					
				default: System.out.println("Wrong Choice");
			}
		}
		
	}
}

class requestHandler extends Thread
{
	final Socket s;
	
	public requestHandler(Socket s)
	{
		this.s = s;
	}
	
	
	@Override
	public void run()
	{
	
		if(!Chord.isAlive)
		{
			return;
		}
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(s.getInputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			String request = dis.readUTF();
			System.out.println(Chord.nodeId + " " + request + "\n");
			if(request.startsWith("find_successor"))
			{
				String[] info = request.split(" ");
				//System.out.println(info[0] + " " + info[1]);
				int nodeId = Integer.parseInt(info[1]);
				///System.out.println(nodeId);
				Chord.findSuccessor(s,nodeId);
				//sleep(5);
				s.close();
				
			}
			if(request.startsWith("find_predecessor"))
			{
				int nodeId = Integer.parseInt((request.split(" "))[1]);
				Chord.findPredecessor(s,nodeId);
				s.close();
			}
			if(request.startsWith("successor"))
			{
				//System.out.println("successor");
				String message = Chord.finger[0].successorId + " " + Chord.finger[0].successorIp.getHostAddress() + " " + Chord.finger[0].successorPort;
				//System.out.println(message);
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(message);
				dos.close();
				s.close();
			}
			if(request.startsWith("predecessor"))
			{
				String message = Chord.predecessorId + " " + Chord.predecessorIp.getHostAddress() + " " + Chord.predecessorPort;
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(message);
				dos.close();
				s.close();
			}
			if(request.startsWith("set_predecessor"))
			{
				String[] info = request.split(" ");
				Chord.predecessorId = Integer.parseInt(info[1]);
				Chord.predecessorIp = InetAddress.getByName(info[2]);
				Chord.predecessorPort = Integer.parseInt(info[3]);
				s.close();
				
			}
			if(request.startsWith("update_finger_table"))
			{
				String[] info = request.split(" ");
				Chord.updateFingerTable(info);
				s.close();
			}
			if(request.startsWith("transfer_file"))
			{
				String[] info = request.split(" ");
				Chord.transferFile(s,info);
				s.close();
			}
			if(request.startsWith("Exit"))
			{
				Chord.updateAfterExit(request);
				s.close();
			}
			if(request.startsWith("StopThread"))
			{
				System.out.println(Chord.nodeId + " has leaved the system.");
				return;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

public class Chord {
	
	public static int predecessorId;
	public static int predecessorPort;
	public static InetAddress predecessorIp;
	
	public static int nodeId;
	public static int nodePort;
	public static InetAddress nodeIp;
	static ServerSocket sock;
	
	public static fingerTable[] finger = new fingerTable[5];
	
	public static boolean isAlive;
	
	
	

	public static int SHA1(String ipAndPort) throws UnsupportedEncodingException, Exception 
	{ 
		String sha1 = null;		
        MessageDigest msdDigest = MessageDigest.getInstance("SHA-1");
        msdDigest.update(ipAndPort.getBytes("UTF-8"), 0, ipAndPort.length());
        sha1 = DatatypeConverter.printHexBinary(msdDigest.digest());
        int key = Integer.parseInt(new BigInteger(sha1, 16).toString(2).substring(95, 100), 2);
		return key;
	}
	
	public static void findSuccessor(Socket s,int otherId) throws IOException
	{
		String predecessorInfo = findPredecessor(otherId);
		String command = "successor";
		
		String[] info = predecessorInfo.split(" ");
		
		Socket ns = new Socket(InetAddress.getByName(info[1]).getHostAddress(),Integer.parseInt(info[2]));
		DataInputStream dis = new DataInputStream(ns.getInputStream());
		DataOutputStream dos = new DataOutputStream(ns.getOutputStream());
		
		dos.writeUTF(command);
		String recieved = dis.readUTF();
		DataOutputStream dos1 = new DataOutputStream(s.getOutputStream());
		dos1.writeUTF(recieved);
		ns.close();
		dos.close();
		dos1.close();
		dis.close();
	}
	
	public static String findPredecessor(int otherId) throws IOException
	{
		
		if(isIntoInterval(otherId,nodeId+1,finger[0].successorId))
			return nodeId + " " + nodeIp.getHostAddress() + " " + nodePort;
		
		int i;
		for(i=4;i>=0;i--)
		{
			if(isIntoInterval(finger[i].successorId,nodeId+1,otherId-1))
				break;
		}
		String command =  "find_predecessor " + otherId;
		Socket s = new Socket(finger[i].successorIp,finger[i].successorPort);
		DataInputStream dis = new DataInputStream(s.getInputStream());
		DataOutputStream dos = new DataOutputStream(s.getOutputStream());
		
		dos.writeUTF(command);
		
		String received = dis.readUTF();
		
		dos.close();
		dis.close();
		s.close();
		
		return received;
		
	}
	
	public static void findPredecessor(Socket s,int otherId) throws IOException
	{
		if(isIntoInterval(otherId,nodeId+1,finger[0].successorId))
		{
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			String message =  nodeId + " " + nodeIp.getHostAddress() + " " + nodePort;
			dos.writeUTF(message);
			return;
		}
		int i;
		for(i=4;i>=0;i--)
		{
			if(isIntoInterval(finger[i].successorId,nodeId+1,otherId-1))
				break;
		}
		String command =  "find_predecessor " + otherId;
		Socket ns = new Socket(finger[i].successorIp,finger[i].successorPort);
		DataInputStream dis = new DataInputStream(ns.getInputStream());
		DataOutputStream dos = new DataOutputStream(ns.getOutputStream());
		
		dos.writeUTF(command);
		
		String received = dis.readUTF();
		DataOutputStream dos1 = new DataOutputStream(s.getOutputStream());
		
		dos1.writeUTF(received);
		
		dos1.close();
		dos.close();
		dis.close();
		ns.close();
		
	}
	
	public static void updateAfterExit(String request)
	{
		if(!Chord.isAlive) return;
		
		String[] info = request.split(" ");
		int idExited = Integer.parseInt(info[1]);
		if(nodeId == idExited) return;
		
		int count = 0;
		
		for(int i=0;i<5;i++)
		{
			if(finger[i].successorId == idExited)
			{
				finger[i].successorId = Integer.parseInt(info[2]);
				try {
					finger[i].successorIp = InetAddress.getByName(info[3]);
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				finger[i].successorPort = Integer.parseInt(info[4]);
				count++;
			}
		}
		if(count == 0) return;
		
		String message = request;
		Socket s;
		try {
			s = new Socket(predecessorIp, predecessorPort);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return;
		}
		DataOutputStream dos;
		try {
			dos = new DataOutputStream(s.getOutputStream());
			dos.writeUTF(message);
			dos.close();
			s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	// processing exit query
	public static void handleExit() throws IOException
	{
		Chord.isAlive = false;
		System.out.println(Chord.nodeId + " is leaving....\n\n");
		
		// updating successor's predecessor
		String message = "set_predecessor " + predecessorId + " " + predecessorIp.getHostAddress() + " " + predecessorPort;
		Socket s = new Socket(finger[0].successorIp, finger[0].successorPort);
		DataOutputStream dos = new DataOutputStream(s.getOutputStream());
		dos.writeUTF(message);
		dos.close();
		s.close();
		
		// informing predecessor to update it's finger table
		message = "Exit " + nodeId + " " + finger[0].successorId + " " + finger[0].successorIp.getHostAddress() + " " + finger[0].successorPort;
		
		s = new Socket(predecessorIp, predecessorPort);
		
		DataOutputStream dos1 = new DataOutputStream(s.getOutputStream());
		dos1.writeUTF(message);
		dos1.close();
		s.close();
		
		// stopping the request handling threads
		message = "StopThread";
		s = new Socket(nodeIp,nodePort);
		dos1.writeUTF(message);
		dos1.close();
		s.close();
		
	}
	
	// tranfering files to the new node from it's successor 
	public static void transferFile(Socket s,String[] info) throws UnsupportedEncodingException, Exception
	{
		int l = Integer.parseInt(info[1]);
		int r = Integer.parseInt(info[2]);
		
		File f = new File("./files" + Integer.toString(nodeId));
		File[] files = f.listFiles();
		ArrayList<File> fileToTransfer = new ArrayList<File>();
		
		for(int i=0;i<files.length;i++)
		{
			String filename = files[i].getName();
			
			int key = SHA1(filename.substring(0, filename.length()-3));
			if(isIntoInterval(key, l, r))
			{
				fileToTransfer.add(files[i]);
			}
		}
		DataOutputStream dos = new DataOutputStream(s.getOutputStream());
		dos.writeUTF(Integer.toString(fileToTransfer.size()));
		
		for(int i=0;i<fileToTransfer.size();i++)
		{
			String filename = fileToTransfer.get(i).getName();
			dos.writeUTF(filename);
			fileToTransfer.get(i).delete();
		}
		dos.close();
		
	}
	
	// checking for nodeId to lie in a given range
	
	public static boolean isIntoInterval(int node,int lnode,int rnode)
	{
		
		if(lnode == rnode)
			return (node == lnode);
		if(lnode < rnode) return ((node >= lnode) && (node <= rnode));
		
		rnode = rnode + 32;
		if(node < lnode) node = node + 32;
		return (node >= lnode) && (node <= rnode);
	}
	
	

	private static String getName() {
		// TODO Auto-generated method stub
		String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
	}

	// first node in the ring
	public static void createRing(String[] args) throws Exception
	{
		InetAddress ip = InetAddress.getByName("localhost");
		int port = Integer.parseInt(args[0]);
		int id = SHA1(ip.getHostAddress() + args[0]);
		
		sock = new ServerSocket(port);
		nodeIp = ip;
		nodePort = port;
		nodeId = id;
		isAlive = true;
		
		predecessorId = nodeId;
		predecessorIp = nodeIp;
		predecessorPort = nodePort;
		
		for(int i=0;i<5;i++)
		{
			int start = (nodeId + (int)Math.pow(2,i))%32;
			int end = (start + (int)Math.pow(2,i))%32;
			finger[i] = new fingerTable(start,end,nodeId,nodePort,nodeIp);
		}
		
		System.out.println("Node " + nodeId + " is ready");
		
		Thread t = new query();
		t.start();
		
		// generating 100 files
		File dir = new File("./files" + Integer.toString(nodeId));
		if(!dir.exists())
			dir.mkdir();
		else
		{
			String[]entries = dir.list();
			for(String s: entries){
    		File currentFile = new File(dir.getPath(),s);
    		currentFile.delete();
    		}
		}
		for(int i = 0;i < 100;i++)
			new File(dir, getName() + ".txt").createNewFile();
		
		listenPort(sock);
		
		
	}
	
	
	// adding node to the ring
	public static void addNodeToRing(String[] args) throws Exception
	{
		
		isAlive = true;
		nodePort = Integer.parseInt(args[0]);
		nodeIp = InetAddress.getLocalHost();
		
		nodeId = SHA1(nodeIp.getHostName() + args[0]);
		
		ServerSocket sock = new ServerSocket(nodePort);
		
		int otherPort = Integer.parseInt(args[1]);
		InetAddress otherIp = InetAddress.getByName(args[2]);
		int otherId = SHA1(otherIp.getHostName() + args[1]);
		//System.out.println(otherId + " " + otherPort + " " + otherIp.getHostAddress());
		init_finger_table(otherPort,otherId,otherIp);
		update_others();
		transfer_file();
		
		System.out.println("Node " + nodeId + " is ready");
		
		Thread t = new query();
		t.start();
		listenPort(sock);
		
	}
	
	// initializing finger table of new node
	public static void init_finger_table(int otherPort,int otherId,InetAddress otherIp) throws IOException
	{
		// updating successor of new node
		
		Socket s = new Socket(otherIp,otherPort);
		DataInputStream dis = new DataInputStream(s.getInputStream());
		DataOutputStream dos = new DataOutputStream(s.getOutputStream());
		String command = "find_successor " + ((nodeId + 1)%32);
		
		dos.writeUTF(command);
		String received = dis.readUTF();
		String[] info = received.split(" ");
		finger[0] = new fingerTable((nodeId+1)%32,(nodeId+2)%32,Integer.parseInt(info[0]),Integer.parseInt(info[2]),InetAddress.getByName(info[1]));
		s.close();
		dos.close();
		dis.close();
		
		// updating predecessor of new node
		command = "predecessor";
		s = new Socket(finger[0].successorIp,finger[0].successorPort);
		dos = new DataOutputStream(s.getOutputStream());
		dos.writeUTF(command);
		dis = new DataInputStream(s.getInputStream());
		received = dis.readUTF();
		info = received.split(" ");
		predecessorId = Integer.parseInt(info[0]);
		predecessorIp = InetAddress.getByName(info[1]);
		predecessorPort = Integer.parseInt(info[2]);
		System.out.println("Successor is " + finger[0].successorId + "Predecessor is " + predecessorId);
		s.close();
		dos.close();
		dis.close();
		
		
		command  = "set_predecessor " + nodeId + " " + nodeIp.getHostAddress() + " " + nodePort;
		s = new Socket(finger[0].successorIp,finger[0].successorPort);
		dos = new DataOutputStream(s.getOutputStream());
		dos.writeUTF(command);
		dos.close();
		s.close();
		
		for(int i=1;i<5;i++)
		{
			int start = (nodeId + (int)Math.pow(2,i))%32;
			int end = (start + (int)Math.pow(2, i))%32;
			
			if(isIntoInterval(start,nodeId,finger[i-1].successorId))
				finger[i] = new fingerTable(start,end,finger[i-1].successorId,finger[i-1].successorPort,finger[i-1].successorIp);
			else
			{
				command = "find_successor " + start;
				s = new Socket(otherIp,otherPort);
				dos = new DataOutputStream(s.getOutputStream());
				dis = new DataInputStream(s.getInputStream());
				
				dos.writeUTF(command);
				received = dis.readUTF();
				
				info  = received.split(" ");
				if(isIntoInterval(nodeId,start,Integer.parseInt(info[0])))
					finger[i] = new fingerTable(start,end,nodeId,nodePort,nodeIp);
				else
					finger[i] = new fingerTable(start,end,Integer.parseInt(info[0]),Integer.parseInt(info[2]),InetAddress.getByName(info[1]));
				dos.close();
				dis.close();
				s.close();
				
			}
			
		}
		
	}
	
	
	
	public static void transfer_file() throws IOException
	{
		int l = predecessorId + 1;
		int r = nodeId;
		
		File dir = new File("./files" + Integer.toString(nodeId));
		if(!dir.exists())
			dir.mkdir();
		else
		{
			String[] entries = dir.list();
			for(String s: entries)
				new File(dir,s).delete();
		}
		System.out.println("Range " + l+ " " + r);
		String message = "transfer_file " + l + " " + r;
		Socket s = new Socket(finger[0].successorIp,finger[0].successorPort);
		DataOutputStream dos = new DataOutputStream(s.getOutputStream());
		dos.writeUTF(message);
		DataInputStream dis = new DataInputStream(s.getInputStream());
		
		int size = Integer.parseInt(dis.readUTF());
		
		for(int i=1;i<=size;i++)
		{
			String filename = dis.readUTF();
			File f = new File("./files" + Integer.toString(nodeId) +"/" + filename);	
			f.createNewFile();
			
		}
		dos.close();
		dis.close();
		s.close();
		
		
	}
	
	// update the finger table if id is the (i)'th finger of nodeId 
	public static void updateFingerTable(String[] info) throws IOException
	{
		int id = Integer.parseInt(info[1]);
		int port = Integer.parseInt(info[3]);
		InetAddress ip = InetAddress.getByName(info[2]);
		int i = Integer.parseInt(info[4]);
		System.out.println("Update1 " + nodeId + " " + id + " " + i + " " + finger[i].start);
		if(finger[i].start == finger[i].successorId) 
			return;
		if(isIntoInterval(id,finger[i].start,finger[i].successorId-1))
		{
			
			finger[i].successorId = id;
			finger[i].successorIp = ip;
			finger[i].successorPort = port;
			
			// updating predecessor of nodeId
			
			String command = "update_finger_table " + id + " " + ip.getHostAddress() + " " + port + " " +  i;
			Socket s = new Socket(predecessorIp,predecessorPort);
			
			
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			dos.writeUTF(command);
			dos.close();
			s.close();
		}
		
	}
	
	//updating other finger table after addition of new node
	public static void update_others() throws IOException
	{
		for(int i=0;i<5;i++)
		{
			int p = (nodeId - (int)Math.pow(2,i) + 1);
			
			if(p < 0) p = p + 32;
			
			String received = findPredecessor(p);
			String[] info = received.split(" ");
			String message = "update_finger_table " + nodeId + " " + nodeIp.getHostAddress() + " " + nodePort + " " +  i;
			Socket s = new Socket(InetAddress.getByName(info[1]),Integer.parseInt(info[2]));
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			dos.writeUTF(message);
			
			dos.close();
			s.close();
		}
	}
	
	
	
	
	public static void listenPort(ServerSocket sock) throws InterruptedException
	{
		while(true)
		{
			if(!Chord.isAlive)
			{
				try {
					sock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}
			Socket s = null;
			try {
				s = sock.accept();
			} catch (IOException e) {
				
				
				return;
			}
			Thread t = new requestHandler(s);
			t.start();
		}
	}
	

	
	public static void main(String[] args) throws Exception
	{
		if(args.length == 0)
		{
			System.out.println("Too few arguements\n");
			return;
		}
		// first node in the ring
		if(args.length == 1)
		{
			createRing(args);
		}
		else
		{
			// ring has been created
			addNodeToRing(args);
			
		}
	}

}

