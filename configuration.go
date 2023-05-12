package papillon

import (
	"encoding/json"
	"fmt"
)

type (
	ClusterInfo struct {
		Servers []ServerInfo
	}
	cluster struct {
		latest      ClusterInfo
		latestIndex uint64
		commit      ClusterInfo
		commitIndex uint64
	}
)

func (c *ClusterInfo) Clone() (copy ClusterInfo) {
	copy.Servers = append([]ServerInfo(nil), c.Servers...)
	return
}

func (c *cluster) Clone() cluster {
	return cluster{
		commit:      c.commit.Clone(),
		latest:      c.latest.Clone(),
		commitIndex: c.commitIndex,
		latestIndex: c.latestIndex,
	}
}

// stable 集群配置是否处在变更阶段
func (c *cluster) stable() bool {
	return c.commitIndex > 0 && c.latestIndex == c.commitIndex
}

// hasVoter 判断一个节点是否有选举权
func (c *ClusterInfo) hasVoter(id ServerID) bool {
	for _, server := range c.Servers {
		if server.ID == id {
			return server.isVoter()
		}
	}
	return false
}
func DecodeCluster(data []byte) (c ClusterInfo) {
	if err := json.Unmarshal(data, &c); err != nil {
		panic(fmt.Errorf("failed to decode ClusterInfo: %s ,%s", err, data))
	}
	return
}
func EncodeCluster(c ClusterInfo) (data []byte) {
	data, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Errorf("failed to encode ClusterInfo :%s", err))
	}
	return
}
func (c *cluster) setCommit(index uint64, configuration ClusterInfo) {
	c.commitIndex = index
	c.commit = configuration
}
func (c *cluster) setLatest(index uint64, configuration ClusterInfo) {
	c.latestIndex = index
	c.latest = configuration
}

// validateConfiguration 校验配置是否合法 1. 可选举节点数大于 0 2. 节点不能重复
func validateConfiguration(configuration ClusterInfo) bool {
	var (
		voter int
		set   = map[ServerID]bool{}
	)
	for _, server := range configuration.Servers {
		if set[server.ID] {
			return false
		}
		set[server.ID] = true
		if server.isVoter() {
			voter++
		}
	}
	return voter > 0
}
func (rc *ReloadableConfig) apply(to Config) Config {
	to.TrailingLogs = rc.TrailingLogs
	to.SnapshotInterval = rc.SnapshotInterval
	to.SnapshotThreshold = rc.SnapshotThreshold
	to.HeartbeatTimeout = rc.HeartbeatTimeout
	to.ElectionTimeout = rc.ElectionTimeout
	return to
}
