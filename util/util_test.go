package util

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

func TestTimestampAfterTimestamp(t *testing.T) {
	tests := map[string]struct {
		timestamp1 string
		timestamp2 string
		want       bool
		wantErr    bool
	}{
		"timestamp1BadFormat": {"2024-01-02T18:37Z", "2024-01-02T18:16:37Z", false, true},
		"timestamp2BadFormat": {"2024-01-02T18:16:37Z", "2024-01-02T18:37Z", false, true},
		"timestamp1After":     {"2024-01-02T18:17:37Z", "2024-01-02T18:16:37Z", true, false},
		"timestamp1NotAfter":  {"2024-01-02T18:16:37Z", "2024-01-02T18:17:37Z", false, false},
		"sameTime":            {"2024-01-02T18:16:37Z", "2024-01-02T18:16:37Z", false, false},
	}

	assert := assert.New(t)
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := TimestampAfterTimestamp(tc.timestamp1, tc.timestamp2)
			assert.Equal(tc.want, got)
			if tc.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}
		})
	}
}
