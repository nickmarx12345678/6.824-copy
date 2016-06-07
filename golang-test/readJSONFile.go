package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
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
	/*
	fileLineContents := getFileLineContents(fileName)
	return jsonDecode(fileLineContents)
	*/
	fileContents, _ := ioutil.ReadFile(fileName)
	lines := strings.Split(string(fileContents), "\n")
	outputBuffer := make([]map[string]string, len(lines))
	for i := 0; i < len(lines); i++ {
		json.Unmarshal([]byte(lines[i]), &outputBuffer[i])
		//fmt.Println(outputBuffer[i])
		keyValue := make(map[string]string)
		json.Unmarshal([]byte(lines[i]), &keyValue)
		fmt.Println(keyValue)
	}
	return outputBuffer
}

func jsonEncode(value interface{}) string {
	jsonBytes, _ := json.Marshal(value)
	return string(jsonBytes)
}

func prettyPrint(data []map[string]string) string {
	output := ""
	for i := 0; i < len(data); i++ {
		x, _ := json.MarshalIndent(data[i], "", "    ")
		output += string(x)
	}
	return output
}

func appendToFile(fileName string, inputString string) (int, error) {
	fileHandle, _ := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND, 0666)
	defer fileHandle.Close()
	num, err := fileHandle.WriteString(inputString)
	return num, err
}

func main() {
	data := jsonDecodeFileLineContents("testFile.txt")
	fmt.Println(prettyPrint(data))
	/*
	fileContents, _ := ioutil.ReadFile("testFile.txt")
	fmt.Println(string(fileContents))
	jsonData := map[string]string{
		"new key": "new item",
	}
	appendToFile("testFile.txt", "\n"+jsonEncode(jsonData))
	jsonBytes, _ := json.Marshal(map[string]string{
		"something": "something else",
	})
	appendToFile("testFile.txt", "\n"+string(jsonBytes))
	*/
}
