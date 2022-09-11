package tool

func GenerateGraph(pairLs [][]string) map[string][]string {
	graph := make(map[string][]string)
	for _, ele := range pairLs {
		value, exists := graph[ele[0]]
		if exists {
			graph[ele[0]] = append(value, ele[1])
		} else {
			graph[ele[0]] = []string{ele[1]}
		}
	}
	return graph
}