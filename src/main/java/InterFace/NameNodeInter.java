package InterFace;

import common.BlockInfo;
import common.Inode;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface NameNodeInter extends Remote {
    boolean heartBeat(String nodeName) throws RemoteException;
    List<List<String>> allocateBlocks(String filePath, int numBlocks) throws RemoteException;

    List<BlockInfo> getBlocks(String filePath) throws RemoteException;

    Inode createFile(String path, String owner) throws RemoteException;
    Inode createDirectory(String path, String owner) throws RemoteException;
    Inode renameFile(String path, String newFileName, String user) throws RemoteException;

    // 重命名目录的辅助方法
    Inode renameDirectory(String oldPath, String newDirPath, String user) throws RemoteException;
    List<String> listFiles(String path) throws RemoteException;
    String getFileInfo(String path) throws RemoteException;

}