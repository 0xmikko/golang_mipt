package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
)

type Printable interface {
	Print(io.Writer, string, bool)
}

type File struct {
	Name string
	Size int64
}

func (f *File) Print(out io.Writer, indent string, last bool) {

	printLeaf(out, indent, f.ToString(), last)

}

func (f *File) ToString() string {
	if f.Size == 0 {
		return fmt.Sprintf("%s (empty)", f.Name)
	} else {
		return fmt.Sprintf("%s (%db)", f.Name, f.Size)
	}
}

type Directory struct {
	Name    string
	Root    bool
	subDirs []*Directory
	files   []*File
}

func (dir *Directory) Print(out io.Writer, indent string, last bool) {

	length := len(dir.subDirs) + len(dir.files)

	itemsToPrint := make(map[string]Printable, length)
	keys := make([]string, 0, length)

	for _, directory := range dir.subDirs {
		itemsToPrint[directory.Name] = directory
		keys = append(keys, directory.Name)
	}

	for _, file := range dir.files {
		itemsToPrint[file.Name] = file
		keys = append(keys, file.Name)
	}

	sort.Strings(keys)

	if !dir.Root {
		printLeaf(out, indent, dir.Name, last)
		if !last {
			indent += "│"
		}
		indent += "\t"
	}

	for num, key := range keys {
		item := itemsToPrint[key]
		item.Print(out, indent, num == length-1)

	}

}

func (dir *Directory) loadDirectoryTree(pathName string, root bool, collectFiles bool) error {

	dir.Root = root
	dirContent, err := ioutil.ReadDir(pathName)
	if err != nil {
		return err
	}

	for _, fileInfo := range dirContent {

		switch fileInfo.IsDir() {

		case true:
			newDir := new(Directory)
			newDir.Name = fileInfo.Name()

			err = newDir.loadDirectoryTree(path.Join(pathName, fileInfo.Name()), false, collectFiles)
			if err != nil {
				return err
			}
			dir.subDirs = append(dir.subDirs, newDir)

		case false:
			if collectFiles {
				newFile := &File{fileInfo.Name(), fileInfo.Size()}
				dir.files = append(dir.files, newFile)
			}
		}

	}
	return nil
}

func printLeaf(out io.Writer, indent, content string, last bool) {
	if !last {
		fmt.Fprintf(out, "%s├───%s\n", indent, content)
	} else {
		fmt.Fprintf(out, "%s└───%s\n", indent, content)
	}
}

func dirTree(out io.Writer, pathName string, printFiles bool) error {

	var rootDir = &Directory{}
	err := rootDir.loadDirectoryTree(pathName, true, printFiles)

	if err != nil {
		return err
	}

	rootDir.Print(out, "", true)
	return nil
}

func main() {
	out := os.Stdout
	if !(len(os.Args) == 2 || len(os.Args) == 3) {
		panic("usage go run main.go . [-f]")
	}
	path := os.Args[1]
	printFiles := len(os.Args) == 3 && os.Args[2] == "-f"
	err := dirTree(out, path, printFiles)
	if err != nil {
		panic(err.Error())
	}
}
