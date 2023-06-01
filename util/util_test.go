package util

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	. "gopkg.in/check.v1"
)

const (
	TestErrErrorFmt  = "Unexpected error for test case: %s: %v"
	TestErrResultFmt = "Unexpected result for test case: %s"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestConvertSize(t *testing.T) {
	assert := require.New(t)

	size, err := ConvertSize("0m")
	assert.Nil(err)
	assert.Equal(int64(0), size)

	size, err = ConvertSize("0Mi")
	assert.Nil(err)
	assert.Equal(int64(0), size)

	size, err = ConvertSize("1024k")
	assert.Nil(err)
	assert.Equal(int64(1024*1000), size)

	size, err = ConvertSize("1024Ki")
	assert.Nil(err)
	assert.Equal(int64(1024*1024), size)

	size, err = ConvertSize("1024")
	assert.Nil(err)
	assert.Equal(int64(1024), size)

	size, err = ConvertSize("1Gi")
	assert.Nil(err)
	assert.Equal(int64(1024*1024*1024), size)

	size, err = ConvertSize("1G")
	assert.Nil(err)
	assert.Equal(int64(1e9), size)
}

func TestRoundUpSize(t *testing.T) {
	assert := require.New(t)

	assert.Equal(int64(SizeAlignment), RoundUpSize(0))
	assert.Equal(int64(2*SizeAlignment), RoundUpSize(SizeAlignment+1))
}

func TestDeterministicUUID(t *testing.T) {
	assert := require.New(t)

	dataUsedToGenerate := "Each time DeterministicUUID is called on this data, it outputs the same UUID."
	assert.Equal(DeterministicUUID(dataUsedToGenerate), DeterministicUUID(dataUsedToGenerate))
}
