package scheduler

import "github.com/ef-ds/deque"

type dag map[string][]string

func (d dag) addNode(name string) {
	deps := make([]string, 0)
	d[name] = deps
}

func (d dag) setDownstream(ind, dep string) {
	d[ind] = append(d[ind], dep)
}

func (d dag) isDownstream(nodeName string) bool {
	ind := d.independentNodes()

	for _, name := range ind {
		if nodeName == name {
			return false
		}
	}

	return true
}

func (d dag) validate() bool {
	degree := make(map[string]int)

	for node := range d {
		degree[node] = 0
	}

	for _, ds := range d {
		for _, i := range ds {
			degree[i]++
		}
	}

	var deq deque.Deque

	for node, val := range degree {
		if val == 0 {
			deq.PushFront(node)
		}
	}

	l := make([]string, 0)

	for {
		popped, ok := deq.PopBack()

		if !ok {
			break
		} else {
			node := popped.(string)
			l = append(l, node)
			for ds := range d {
				degree[ds]--
				if degree[ds] == 0 {
					deq.PushFront(ds)
				}
			}
		}
	}

	return len(l) == len(d)
}

func (d dag) dependencies(node string) []string {

	dependencies := make([]string, 0)

	for dep, ds := range d {
		for _, i := range ds {
			if node == i {
				dependencies = append(dependencies, dep)
			}
		}
	}

	return dependencies
}

func (d dag) independentNodes() []string {

	downstream := make([]string, 0)

	for _, ds := range d {
		downstream = append(downstream, ds...)
	}

	ind := make([]string, 0)

	for node := range d {
		ctr := 0
		for _, i := range downstream {
			if node == i {
				ctr++
			}
		}
		if ctr == 0 {
			ind = append(ind, node)
		}
	}

	return ind
}
