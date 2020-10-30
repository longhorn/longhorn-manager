package meta

const (
	APIVersion    = 1
	APIMinVersion = 1
)

var (
	Version   string
	GitCommit string
	BuildDate string
)

type VersionOutput struct {
	Version   string `json:"version"`
	GitCommit string `json:"gitCommit"`
	BuildDate string `json:"buildDate"`

	APIVersion    int `json:"apiVersion"`
	APIMinVersion int `json:"apiMinVersion"`
}

func GetVersion() VersionOutput {
	return VersionOutput{
		Version:   Version,
		GitCommit: GitCommit,
		BuildDate: BuildDate,

		APIVersion:    APIVersion,
		APIMinVersion: APIMinVersion,
	}
}
