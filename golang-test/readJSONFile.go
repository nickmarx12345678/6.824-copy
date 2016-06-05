package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func countNumLinesFromFile(file *os.File) int {
	bufferSize := 1024
	byteBuffer := make([]byte, bufferSize)
	var linesCount int
	offset := 0
	for {
		n, err := file.ReadAt(byteBuffer, int64(offset))
		linesCount += bytes.Count(byteBuffer, []byte{'\n'})
		if n == 0 || err == io.EOF {
			break
		}
		offset += bufferSize
	}
	// This is because counting by how many newline bytes.
	return linesCount + 1
}

func getFileLineContents(fileName string) [][]byte {
	fileHandle, _ := os.Open(fileName)
	defer fileHandle.Close()
	numLines := countNumLinesFromFile(fileHandle)
	outputBuffer := make([][]byte, numLines)
	index := 0
	// GOTCHA: Scanner has size limits for line itself and number of lines.
	// If running outside of constraints, use Reader or readAt().
	scanner := bufio.NewScanner(fileHandle)
	for scanner.Scan() {
		outputBuffer[index] = scanner.Bytes()
		index++
	}
	// This doesn't currently work.
	// GOTCHA: ReadString() INCLUDES THE DELIMITER WITH RETURNED STRING!!!
	// MOST LIKELY WANT TO STRIP THE ENDING DELIMITER!!!
	/*
		reader := bufio.NewReader(fileHandle)
		for {
			line, err := reader.ReadString('\n')
			outputBuffer[index] = line
			if err == io.EOF {
				break
			}
		}
	*/
	return outputBuffer
}

func jsonDecode(jsonBytes [][]byte) []map[string]string {
	outputBuffer := make([]map[string]string, len(jsonBytes))
	for i := 0; i < len(jsonBytes); i++ {
		json.Unmarshal(jsonBytes[i], &outputBuffer[i])
		//fmt.Println(jsonBytes[i])
	}
	return outputBuffer
}

func jsonDecodeFileLineContents(fileName string) []map[string]string {
	fileLineContents := getFileLineContents(fileName)
	return jsonDecode(fileLineContents)
}

func prettyPrint(data []map[string]string) string {
	output := ""
	for i := 0; i < len(data); i++ {
		x, _ := json.MarshalIndent(data[i], "", "    ")
		output += string(x)
	}
	return output
}

func main() {
	data := jsonDecodeFileLineContents("testFile.txt")
	fmt.Println(prettyPrint(data))
}
