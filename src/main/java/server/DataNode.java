package server;

import InterFace.DataNodeInter;
import InterFace.NameNodeInter;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class DataNode extends UnicastRemoteObject implements DataNodeInter {
    private NameNodeInter nameNode;
    private final String name;
    private Map<String, String> fileStorage; // 模拟存储块
    private Timer timer; // 用于定时任务

    public DataNode(String name, NameNodeInter nameNode) throws RemoteException {
        super();
        this.name = name;
        this.nameNode = nameNode; // 假设在构造函数中传入NameNodeInter的实现
        this.fileStorage = new HashMap<>();
        this.timer = new Timer(); // 初始化定时器
        scheduleHeartBeat(); // 启动定时心跳任务
    }

    // 定时心跳任务
    private void scheduleHeartBeat() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    boolean heartBeatResult = nameNode.heartBeat(name);
                    if (!heartBeatResult) {
                        System.out.println("Heartbeat failed, trying to reconnect...");
                        // 这里可以添加重连逻辑
                    }
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 10000); // 每10秒执行一次
    }

    @Override
    public boolean uploadFile(String blockId, String data) throws RemoteException {
        fileStorage.put(blockId, data);
        System.out.println("Block " + blockId + " uploaded to " + name);
        return true;
    }

    @Override
    public String downloadFile(String blockId) throws RemoteException {
        return fileStorage.get(blockId);
    }

    @Override
    public boolean deleteFile(String fileId) throws RemoteException {
        fileStorage.remove(fileId);
        return true;
    }

    public static void main(String[] args) {
        try {
            NameNodeInter nameNode = (NameNodeInter) Naming.lookup("//localhost/nameNode"); // 假设NameNodeInter的实现已经在RMI注册
            DataNode dataNode1 = new DataNode("dataNode1", nameNode);
            DataNode dataNode2 = new DataNode("dataNode2", nameNode);
            DataNode dataNode3 = new DataNode("dataNode3", nameNode);
            Naming.rebind("//localhost/dataNode1", dataNode1);
            Naming.rebind("//localhost/dataNode2", dataNode2);
            Naming.rebind("//localhost/dataNode3", dataNode3);
            System.out.println("DataNode is ready.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}