package util

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvertSize(t *testing.T) {
	assert := require.New(t)

	size, err := ConvertSize("0b")
	assert.Nil(err)
	assert.Equal(int64(0), size)

	size, err = ConvertSize("1024b")
	assert.Nil(err)
	assert.Equal(int64(1024), size)

	size, err = ConvertSize("1024")
	assert.Nil(err)
	assert.Equal(int64(1024), size)
}

func TestRoundUpSize(t *testing.T) {
	assert := require.New(t)

	assert.Equal(int64(4096), RoundUpSize(0))
	assert.Equal(int64(8192), RoundUpSize(2323+4096))
}

func TestReplicaName(t *testing.T) {
	assert := require.New(t)

	assert.Equal("replica-XX", ReplicaName("tcp://replica-XX:9502", "tt"))
	assert.Equal("replica-XX", ReplicaName("tcp://replica-XX.rancher.internal:9502", "tt"))
	assert.Equal("replica-XX", ReplicaName("tcp://replica-XX.volume-tt:9502", "tt"))
}
