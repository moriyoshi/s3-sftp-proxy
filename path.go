package main

import "strings"

type Path []string

func splitIntoPathInner(p Path, path string, state int) Path {
	s := 0
	i := 0
	c := 0
	for c >= 0 {
		if i < len(path) {
			c = int(path[i])
		} else {
			c = -1
		}
		switch state {
		case 0:
			if c == '/' {
				i++
			} else {
				state = 1
				s = i
			}
		case 1:
			if c == '/' || c < 0 {
				p = append(p, path[s:i])
				state = 0
			} else {
				i++
			}
		}
	}
	return p
}

func SplitIntoPathAsAbs(path string) Path {
	if path == "" {
		return Path{}
	}
	return splitIntoPathInner(Path{""}, path, 0)
}

func SplitIntoPath(path string) Path {
	if path == "" {
		return Path{}
	}
	return splitIntoPathInner(Path{}, path, 1)
}

func (p Path) Canonicalize() Path {
	retval := make(Path, 0, len(p))
	for _, c := range p {
		switch c {
		case ".":
			continue
		case "..":
			if len(retval) > 0 && retval[len(retval)-1] != "" {
				retval = retval[:len(retval)-1]
			}
		default:
			retval = append(retval, c)
		}
	}
	return retval
}

func (p Path) IsEmpty() bool {
	return len(p) == 0
}

func (p Path) IsRoot() bool {
	return len(p) == 1 && p[0] == ""
}

func (p Path) IsAbs() bool {
	return len(p) > 0 && p[0] == ""
}

func (p Path) Join(another Path) Path {
	if len(another) > 0 && another[0] == "" {
		return append(p, another[1:]...)
	} else {
		return append(p, another...)
	}
}

func (p Path) String() string {
	return strings.Join(p, "/")
}

func (p Path) IsPrefixed(another Path) bool {
	if len(p) < len(another) {
		return false
	}
	for i, c := range another {
		if p[i] != c {
			return false
		}
	}
	return true
}

func (p Path) Prefix() Path {
	if len(p) == 0 {
		return p
	} else if len(p) == 1 {
		if p[0] == "" {
			return Path{""}
		} else {
			return Path{}
		}
	} else {
		return p[:len(p)-1]
	}
}

func (p Path) BasePart() Path {
	if len(p) == 0 {
		return p
	} else if len(p) == 1 {
		if p[0] == "" {
			return Path{""}
		} else {
			return Path{}
		}
	} else {
		return p[len(p)-1:]
	}
}

func (p Path) Base() string {
	if len(p) == 0 {
		return ""
	} else if len(p) == 1 {
		if p[0] == "" {
			return "/"
		} else {
			return ""
		}
	} else {
		return p[len(p)-1]
	}
}
