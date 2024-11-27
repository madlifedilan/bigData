package common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Inode implements Serializable {

    private final String id; // 唯一标识符
    private String fileName;
    private long fileSize;
    private final long creationTime;
    private String path;
    private String owner;
    private String group;
    private final boolean isDirectory;
    private String parentId; // 父节点ID
    private List<String> childrenIds = new ArrayList<>(); // 子节点ID列表
    private boolean status = false;
    private List<BlockInfo> blocks; // 每个文件的块信息

    public Inode(String path, String owner, boolean isDirectory, String parentId) {
        this.id = UUID.randomUUID().toString(); // 生成唯一ID
        this.fileName = null;
        this.fileSize = -1;
        this.creationTime = System.currentTimeMillis();
        this.path = path;
        this.owner = owner;
        this.isDirectory = isDirectory;
        this.parentId = parentId;
        this.blocks = new ArrayList<>();

        // 是文件的情况下
        if (!isDirectory) {
            this.fileName = getFileName(path);
        }
    }

    public String getId() {
        return id;
    }

    public void addBlock(BlockInfo block) {
        this.blocks.add(block);
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    // 从路径中获取文件名
    private String getFileName(String path) {
        int lastSeparatorIndex = path.lastIndexOf('/');
        return lastSeparatorIndex == -1 ? path : path.substring(lastSeparatorIndex + 1);
    }

    public String getFileName() {
        return fileName;
    }

    // 修改文件名
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getPath() {
        return path;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public String getParentId() {
        return parentId;
    }

    public List<String> getChildrenIds() {
        return childrenIds;
    }

    public void addChildId(String childId) {
        this.childrenIds.add(childId);
    }

    public boolean getStatus() {
        return status;
    }

    public void setStatus(Boolean status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FileInfo{" +
                "id='" + id + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", creationTime=" + creationTime +
                ", path='" + path + '\'' +
                ", owner='" + owner + '\'' +
                ", group='" + group + '\'' +
                ", isDirectory=" + isDirectory +
                ", parentId='" + parentId + '\'' +
                ", childrenIds=" + childrenIds +
                '}';
    }

    private String getParentPath() {
        int lastSeparatorIndex = path.lastIndexOf('/');
        if (lastSeparatorIndex == 0) {
            // 根目录或根文件情况下，父目录设置为空字符串
            return "";
        } else if (lastSeparatorIndex == -1) {
            // 没有父目录，返回空字符串
            return "";
        } else {
            // 返回最后一个'/'之前的子字符串，即父目录路径
            return path.substring(0, lastSeparatorIndex);
        }
    }

    public void setPath(String newPath) {
        this.path = newPath;
    }
}