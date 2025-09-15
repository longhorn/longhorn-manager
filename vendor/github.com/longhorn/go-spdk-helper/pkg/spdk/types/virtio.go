package types

type BdevVirtioAttachControllerRequest struct {
	Name    string `json:"name"`
	Trtype  string `json:"trtype"`
	Traddr  string `json:"traddr"`
	DevType string `json:"dev_type"`
}

type BdevVirtioDetachControllerRequest struct {
	Name string `json:"name"`
}
