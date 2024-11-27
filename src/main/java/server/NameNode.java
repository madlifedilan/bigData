package server;

import InterFace.NameNodeInter;
import common.BlockInfo;
import common.Inode;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class NameNode extends UnicastRemoteObject implements NameNodeInter {
    private Map<String, Inode> idToInodeMap; // ID到Inode的映射
    private Map<String, Inode> pathToInodeMap; // 路径到Inode的映射
    private List<String> dataNodes; // 存储活跃的DataNode名称
    private Map<String, Long> lastHeartbeatTime; // 存储每个DataNode的最后一次心跳时间
    private static final long HEARTBEAT_TIMEOUT = 30000; // 心跳超时时间，30秒
    private Inode root; // 根目录
    private Timer timer; // 定时器

    public NameNode() throws RemoteException {
        super();
        idToInodeMap = new HashMap<>();
        pathToInodeMap = new HashMap<>();
        dataNodes = new ArrayList<>();
        lastHeartbeatTime = new HashMap<>();
        root = createDirectory("/", "root");
        timer = new Timer(); // 初始化定时器
        scheduleRemoveInactiveDataNodes(); // 启动定时任务
    }

    @Override
    public boolean heartBeat(String nodeName) throws RemoteException {
        synchronized (this) {
            long currentTime = System.currentTimeMillis();
            // 更新心跳时间
            lastHeartbeatTime.put(nodeName, currentTime);
            // 打印回显信息
//            System.out.println("Received heartbeat from DataNode: " + nodeName + " at " + new Date(currentTime));

            // 添加到活跃DataNode集合
            if (!dataNodes.contains(nodeName)) {
                dataNodes.add(nodeName);
            }
            return true;
        }
    }

    @Override
    public List<List<String>> allocateBlocks(String filePath, int numBlocks) throws RemoteException {
        List<List<String>> blocks = new ArrayList<>();
        Inode fileInode = pathToInodeMap.get(filePath); // 获取文件的 inode

        if (fileInode == null) {
            System.out.println("File " + filePath + " does not exist.");
            return null;
        }

        for (int i = 0; i < numBlocks; i++) {
            List<String> replicas = new ArrayList<>();
            for (int r = 0; r < 3; r++) { // 默认副本数为 3
                String dataNode = dataNodes.get((i + r) % dataNodes.size());
                replicas.add(dataNode);
            }
            String blockId = fileInode.getId() + "_block_" + i;

            // 创建块信息并关联到文件
            BlockInfo blockInfo = new BlockInfo(blockId, replicas);
            fileInode.addBlock(blockInfo);

            blocks.add(replicas);
        }
        return blocks;
    }

    @Override
    public List<BlockInfo> getBlocks(String filePath) throws RemoteException {
        Inode fileInode = pathToInodeMap.get(filePath);
        if (fileInode == null) {
            System.out.println("File " + filePath + " does not exist.");
            return null;
        }
        return fileInode.getBlocks();
    }

    @Override
    public Inode createFile(String path, String owner) throws RemoteException {
        return create(path, owner, false);
    }

    @Override
    public Inode createDirectory(String path, String owner) throws RemoteException {
        return create(path, owner, true);
    }

    // 重命名文件
    @Override
    public Inode renameFile(String path, String newFileName, String user) throws RemoteException {
        // 构造新的路径
        String parentPath = getParentPath(path); // 获取原文件的父目录路径
        String newFilePath = parentPath + newFileName; // 构造新的完整路径

        // 调用rename方法进行重命名
        return rename(path, newFilePath, user);
    }

    // 重命名目录的辅助方法
    @Override
    public Inode renameDirectory(String oldPath, String newDirPath, String user) throws RemoteException {
        // 检查新路径是否合法，即不包含文件名，只有目录路径
        if (newDirPath.contains("/") && !newDirPath.endsWith("/")) {
            String fileName = getFileName(newDirPath);
            String dirPath = getParentPath(newDirPath);
            return rename(oldPath, dirPath + "/" + fileName, user);
        } else {
            System.out.println("Invalid new directory path. The new path must be a full path including the directory name.");
            return null;
        }
    }

    // 输入文件夹的路径，返回该文件夹下所有文件和文件夹
    @Override
    public List<String> listFiles(String path) throws RemoteException {
        List<String> fileList = new ArrayList<>();
        Inode inode = getInode(path);
        if (inode != null && inode.isDirectory()) {
            for (String childId : inode.getChildrenIds()) {
                Inode childInode = idToInodeMap.get(childId);
                if (childInode != null) {
                    fileList.add(childInode.getPath());
                }
            }
        }

        return fileList;
    }

    @Override
    public String getFileInfo(String path) throws RemoteException {
        Inode inode = getInode(path);
        if (inode != null) {
            return inode.toString();
        }
        return null;
    }


    // 定时移除不活跃的DataNode
    private void scheduleRemoveInactiveDataNodes() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                removeInactiveDataNodes();
            }
        }, HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT); // 每30秒执行一次
    }

    // 移除不活跃的DataNode
    private void removeInactiveDataNodes() {
        long now = System.currentTimeMillis();
        synchronized (this) {
            Iterator<Map.Entry<String, Long>> iterator = lastHeartbeatTime.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                String nodeName = entry.getKey();
                long lastHeartbeat = entry.getValue();
                if (lastHeartbeat + HEARTBEAT_TIMEOUT < now) {
                    dataNodes.remove(nodeName);
                    iterator.remove();
                    System.out.println("DataNode " + nodeName + " has been marked as inactive.");
                }
            }
        }
    }

    // 获取活跃的DataNode列表
    public List<String> getActiveDataNodes() {
        synchronized (this) {
            return new ArrayList<>(dataNodes);
        }
    }

    // 重命名文件或目录
    public Inode rename(String oldPath, String newPath, String user) throws RemoteException {
        Inode inode = getInode(oldPath);
        if (inode != null) {
            if (inode.getOwner().equals(user)) {
                if (!pathToInodeMap.containsKey(newPath)) { // 检查新路径是否已存在
                    Inode parentInode = getInode(getParentPath(newPath)); // 获取新路径的父目录Inode
                    if (parentInode != null) {
                        // 更新Inode的路径和名称
                        inode.setPath(newPath);
                        if(!inode.isDirectory()) {
                            inode.setFileName(getFileName(newPath));
                        }
                        // 从旧路径映射中移除
                        pathToInodeMap.remove(oldPath);
                        // 在新路径映射中添加
                        pathToInodeMap.put(newPath, inode);
                        // 更新父目录的子节点列表
                        if(!parentInode.getChildrenIds().contains(inode.getId())) {
                            parentInode.getChildrenIds().add(inode.getId());
                        }
                        // 如果旧路径和新路径的父目录不同，则从旧父目录的子节点列表中移除
                        Inode oldParentInode = getInode(getParentPath(oldPath));
                        if (oldParentInode != null && !oldParentInode.getId().equals(parentInode.getId())) {
                            oldParentInode.getChildrenIds().remove(inode.getId());
                        }
                        // 递归更新所有子文件和子文件夹的路径
                        renameChildren(inode, oldPath, newPath);
                        System.out.println("Renamed " + oldPath + " to " + newPath);
                        return inode;
                    } else {
                        System.out.println("Parent directory for new path does not exist.");
                    }
                } else {
                    System.out.println("A file or directory with the target name already exists.");
                }
            } else {
                System.out.println("Permission denied. You are not the owner of " + oldPath);
            }
        } else {
            System.out.println("File/Directory " + oldPath + " not found.");
        }
        return null;
    }

    // 递归更新所有子文件和子文件夹的路径
    private void renameChildren(Inode inode, String oldParentPath, String newParentPath) {
        List<String> childrenIds = inode.getChildrenIds();
        for (String childId : childrenIds) {
            Inode childInode = idToInodeMap.get(childId);
            if (childInode != null) {
                String oldChildPath = childInode.getPath();
                String newChildPath = oldChildPath.replace(oldParentPath, newParentPath);
                childInode.setPath(newChildPath);
                pathToInodeMap.put(newChildPath, childInode); // 更新路径映射
                pathToInodeMap.remove(oldChildPath); // 移除旧路径映射
                renameChildren(childInode, oldParentPath, newParentPath); // 递归更新子文件和子文件夹的路径
            }
        }
    }

    // 从路径中获取文件名
    private String getFileName(String path) {
        int lastSeparatorIndex = path.lastIndexOf('/');
        return lastSeparatorIndex == -1 ? path : path.substring(lastSeparatorIndex + 1);
    }

    // 获取文件或目录信息
    public Inode getInode(String path) {
        return pathToInodeMap.get(path);
    }

    // 获取文件或目录信息
    public Inode getInodeById(String id) {
        return idToInodeMap.get(id);
    }

    // 删除文件或目录
    public boolean delete(String path, String user) {
        if (pathToInodeMap.containsKey(path)) {
            Inode inode = pathToInodeMap.get(path);
            String owner = inode.getOwner();
            if (owner.equals(user)) {
                // 删除文件或目录
                Inode parentInode = idToInodeMap.get(inode.getParentId());
                if (parentInode != null) {
                    parentInode.getChildrenIds().remove(inode.getId());
                }
                pathToInodeMap.remove(path);
                idToInodeMap.remove(inode.getId());
                return true;
            } else {
                System.out.println("Permission denied. You are not the owner of " + path);
            }
        } else {
            System.out.println("File/Directory " + path + " not found.");
        }
        return false;
    }


    // 创建文件或目录
    public Inode create(String path, String owner, boolean isDirectory) {
        Inode inode = null;
        Inode parentInode = null;

        if (!pathToInodeMap.containsKey(path)) {
            String parentPath = getParentPath(path);

            // 根目录的父目录应该是空
            if (!path.equals("/")) {
                if (!pathToInodeMap.containsKey(parentPath)) {
                    System.out.println("Parent directory " + parentPath + " does not exist.");
                    // 递归创建父目录
                    parentInode = create(parentPath, owner, true);
                } else {
                    // 如果父路径存在，则直接获取父目录的inode
                    parentInode = pathToInodeMap.get(parentPath);
                }
            }

            // 创建新inode
            inode = new Inode(path, owner, isDirectory, parentInode != null ? parentInode.getId() : null);
            idToInodeMap.put(inode.getId(), inode); // 将新inode加入ID映射
            pathToInodeMap.put(path, inode); // 将新inode加入路径映射
            if (parentInode != null) {
                parentInode.addChildId(inode.getId());
            }
            System.out.println((isDirectory ? "Directory" : "File") + " " + path + " created by " + owner);
        } else {
            System.out.println((isDirectory ? "Directory" : "File") + " " + path + " already exists.");
            inode = pathToInodeMap.get(path);
        }
        return inode;
    }

    // 获取父目录路径
    private String getParentPath(String path) {
        // 路径以'/'开头，表示是根目录或根目录下的文件/目录
        if (path.equals("/")) {
            // 根目录的父目录是空
            return "";
        } else {
            int lastSeparatorIndex = path.lastIndexOf('/');
            // 根目录下的文件或目录，父目录是根目录
            if (lastSeparatorIndex == 0) {
                return "/";
            } else {
                // 返回最后一个'/'之前的子字符串，即父目录路径
                return path.substring(0, lastSeparatorIndex);
            }
        }
    }

    public static void main(String[] args) {
        try {
            NameNode nameNode = new NameNode();
            // 这里启动了一个rpc的服务器
            Registry registry = LocateRegistry.createRegistry(1099);
            Naming.rebind("//localhost/nameNode", nameNode);
            System.out.println("NameNode is ready.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}