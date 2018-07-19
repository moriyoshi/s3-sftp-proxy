package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSplitIntoPath(t *testing.T) {
	assert.Equal(t, Path{}, SplitIntoPath(""))
	assert.Equal(t, Path{"abc"}, SplitIntoPath("abc"))
	assert.Equal(t, Path{"abc", "bcd"}, SplitIntoPath("abc/bcd"))
	assert.Equal(t, Path{"abc", "bcd"}, SplitIntoPath("abc/bcd/"))
	assert.Equal(t, Path{"abc", "bcd"}, SplitIntoPath("abc/bcd///"))
	assert.Equal(t, Path{"abc", "bcd", "cde"}, SplitIntoPath("abc/bcd//cde"))
	assert.Equal(t, Path{""}, SplitIntoPath("/"))
	assert.Equal(t, Path{""}, SplitIntoPath("//"))
	assert.Equal(t, Path{"", "abc", "bcd"}, SplitIntoPath("//abc//bcd"))
}

func TestSplitIntoPathAbsolute(t *testing.T) {
	assert.Equal(t, Path{}, SplitIntoPathAsAbs(""))
	assert.Equal(t, Path{"", "abc"}, SplitIntoPathAsAbs("abc"))
	assert.Equal(t, Path{"", "abc", "bcd"}, SplitIntoPathAsAbs("abc/bcd"))
	assert.Equal(t, Path{"", "abc", "bcd"}, SplitIntoPathAsAbs("abc/bcd/"))
	assert.Equal(t, Path{"", "abc", "bcd"}, SplitIntoPathAsAbs("abc/bcd///"))
	assert.Equal(t, Path{"", "abc", "bcd", "cde"}, SplitIntoPathAsAbs("abc/bcd//cde"))
	assert.Equal(t, Path{""}, SplitIntoPathAsAbs("/"))
	assert.Equal(t, Path{""}, SplitIntoPathAsAbs("//"))
	assert.Equal(t, Path{"", "abc", "bcd"}, SplitIntoPathAsAbs("//abc//bcd"))
}
