package InterFace;


import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInter extends Remote {
    boolean uploadFile(String fileId, String data) throws RemoteException;
    String downloadFile(String fileId) throws RemoteException;
    boolean deleteFile(String fileId) throws RemoteException;
}
