package agent

func SetDeleteDirectories(mockDeleteDirectories func([]string, []string) error) func() {
	original := deleteDirectories
	deleteDirectories = mockDeleteDirectories
	return func() {
		deleteDirectories = original
	}
}
