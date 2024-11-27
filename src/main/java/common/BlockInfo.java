package common;

import java.util.List;

// 定义块信息类
public class BlockInfo implements java.io.Serializable{
    private String blockId;
    private List<String> locations;

    public BlockInfo(String blockId, List<String> locations) {
        this.blockId = blockId;
        this.locations = locations;
    }

    public String getBlockId() {
        return blockId;
    }

    public List<String> getLocations() {
        return locations;
    }
}