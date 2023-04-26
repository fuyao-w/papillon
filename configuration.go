package papillon

import (
	"encoding/json"
	"fmt"
)

type (
	Configuration struct {
		Servers []ServerInfo
	}
	cluster struct {
		latest      Configuration
		latestIndex uint64
		commit      Configuration
		commitIndex uint64
	}
)

func (c *Configuration) Clone() (copy Configuration) {
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

func DecodeConfiguration(data []byte) (c Configuration) {
	if err := json.Unmarshal(data, &c); err != nil {
		panic(fmt.Errorf("failed to decode Configuration: %s ,%s", err, data))
	}
	return
}
func EncodeConfiguration(c Configuration) (data []byte) {
	data, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Errorf("failed to encode Configuration :%s", err))
	}
	return
}
func (c *cluster) setCommit(index uint64, configuration Configuration) {
	c.commitIndex = index
	c.commit = configuration
}
func (c *cluster) setLatest(index uint64, configuration Configuration) {
	c.latestIndex = index
	c.latest = configuration
}

// validateConfiguration 校验配置是否合法 1. 可选举节点数大于 0 2. 节点不能重复
func validateConfiguration(configuration Configuration) bool {
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
