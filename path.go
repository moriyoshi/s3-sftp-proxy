package main

import "strings"

// Path represents a fs path
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

// SplitIntoPathAsAbs splits a path and returns a path object in abs
func SplitIntoPathAsAbs(path string) Path {
	if path == "" {
		return Path{}
	}
	return splitIntoPathInner(Path{""}, path, 0)
}

// SplitIntoPath splits a path and returns a path object
func SplitIntoPath(path string) Path {
	if path == "" {
		return Path{}
	}
	return splitIntoPathInner(Path{}, path, 1)
}

// Canonicalize returns the canonicalized path
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

// IsEmpty returns true if current path is empty
func (p Path) IsEmpty() bool {
	return len(p) == 0
}

// IsRoot returns true if current path is root
func (p Path) IsRoot() bool {
	return len(p) == 1 && p[0] == ""
}

// IsAbs returns true if current path is in abs mode
func (p Path) IsAbs() bool {
	return len(p) > 0 && p[0] == ""
}

// Join joins current path with another (passed as parameter) and returns a new path
func (p Path) Join(another Path) Path {
	if len(another) > 0 && another[0] == "" {
		return append(p, another[1:]...)
	}
	return append(p, another...)
}

// String converts a path into a string
func (p Path) String() string {
	return strings.Join(p, "/")
}

// IsPrefixed returns true if current path is prefixed by another one (passed as parameter)
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

// Prefix returns the prefix of current path
func (p Path) Prefix() Path {
	if len(p) == 0 {
		return p
	}
	if len(p) == 1 {
		if p[0] == "" {
			return Path{""}
		}
		return Path{}
	}
	return p[:len(p)-1]
}

// BasePart returns the base part of current path
func (p Path) BasePart() Path {
	if len(p) == 0 {
		return p
	}
	if len(p) == 1 {
		if p[0] == "" {
			return Path{""}
		}
		return Path{}
	}
	return p[len(p)-1:]
}

// Base returns the base of current path as string
func (p Path) Base() string {
	if len(p) == 0 {
		return ""
	}
	if len(p) == 1 {
		if p[0] == "" {
			return "/"
		}
		return ""
	}
	return p[len(p)-1]
}

// Equal returns true if current path and another one (passed as parameter) are equal
func (p Path) Equal(p2 Path) bool {
	if len(p) != len(p2) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] != p2[i] {
			return false
		}
	}
	return true
}
