package client;

import InterFace.DataNodeInter;
import InterFace.NameNodeInter;
import common.BlockInfo;
import common.Config;
import common.Inode;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.List;

public class Client {
    private NameNodeInter nameNode;

    public Client() {
        try {
            this.nameNode = (NameNodeInter) Naming.lookup("//localhost/nameNode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String readFile(String path, long offset, int len) {
        return null;
    }

    public boolean writeFile(String path, String data) {
        boolean ret = false;
        try {
            // 1. 分块数据
            int blockSize = 5; // 5 byte 块大小
            int numBlocks = (int) Math.ceil((double) data.length() / blockSize);

            // 2. 请求 NameNode 分配 DataNode
            List<List<String>> blockLocations = nameNode.allocateBlocks(path, numBlocks);

            for (int i = 0; i < numBlocks; i++) {
                // 获取当前块的数据
                int start = i * blockSize;
                int end = Math.min((i + 1) * blockSize, data.length());
                String blockData = data.substring(start, end);
                String blockId = path + "_block_" + i;

                // 上传到每个副本对应的 DataNode
                List<String> replicas = blockLocations.get(i);
                for (String replicaNode : replicas) {
                    try {
                        DataNodeInter dataNode = (DataNodeInter) Naming.lookup("//localhost/" + replicaNode);
                        ret &= dataNode.uploadFile(blockId, blockData);
                    } catch (Exception e) {
                        e.printStackTrace();
                        ret = true;
                    }
                }
            }
        } catch (RemoteException e) {
            e.printStackTrace();
            ret = false;
        }
        return ret;
    }

    public void ListFiles(String path) {
        try {
            List<String> fileList = nameNode.listFiles(path);
            if (fileList.isEmpty()) {
                System.out.println("No files found.");
            } else {
                System.out.println("-------------------------");
                System.out.println("Files in the system:");
                for (String file : fileList) {
                    System.out.println(file);
                }
            }
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public long getFileSize(String path) {
        return 0;
    }

    public boolean createFile(String path) {
        Inode inode = null;
        try {
            inode = nameNode.createFile(path, Config.USER);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return inode != null;
    }

    public boolean createDir(String path) {
        Inode inode = null;
        try{
            inode = nameNode.createDirectory(path,Config.USER);
        }catch(RemoteException e){
            e.printStackTrace();
        }
        return inode != null;
    }

    public boolean renameFile(String oldPath, String newFileName) {
        Inode inode = null;
        try{
            inode = nameNode.renameFile(oldPath, newFileName, Config.USER);
        }catch(RemoteException e){
            e.printStackTrace();
        }
        return inode != null;
    }

    public boolean renameDir(String oldPath, String newDirName) {
        Inode inode = null;
        try{
            inode = nameNode.renameDirectory(oldPath, newDirName, Config.USER);
        }catch(RemoteException e){
            e.printStackTrace();
        }
        return inode != null;
    }

    public boolean deleteFile(String path) {
        boolean ret = false;
        return ret;
    }

    public boolean getLocations(String path) {
        List<BlockInfo> bi = null;
        try{
            bi = nameNode.getBlocks(path);
        } catch (RemoteException e) {
            e.printStackTrace();
            return false;

        }
        System.out.println(bi.toString());
        return true;
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.ListFiles("/");
        client.createDir("/a/b/c");
        client.renameDir("/a/b", "/a/f");
        client.createFile("/a/b/c/www.txt");
        client.ListFiles("/");
        client.writeFile("/a/b/c/www.txt", "Hello World         ");
        client.getLocations("/a/b/c/www.txt");
    }
}