package main

import(
	"encoding/json"
	"fmt"
	"io/ioutil"
	"unicode"
)

func getFileContents(fileName string) []byte {
	fileContents, _ := ioutil.ReadFile(fileName)
	return fileContents
}

func getWordCountFromBytes(bytes []byte) map[string]int {
	word := ""
	wordCount := make(map[string]int)
	for i := 0; i < len(bytes); i++ {
		if unicode.IsLetter(rune(bytes[i])) {
			word += string(bytes[i])
		} else {
			if (len(word) >= 1) {
				_, keyExists := wordCount[word]
				if (keyExists) {
					wordCount[word]++
				} else {
					wordCount[word] = 1
				}
			}
			word = ""
		}
	}
	return wordCount
}

func prettyPrint(data map[string]int) string {
	x, _ := json.MarshalIndent(data, "", "    ")
	return string(x)
	/*
	output := ""
	for i := 0; i < len(data); i++ {
		x, _ := json.MarshalIndent(data[i], "", "    ")
		output += string(x)
	}
	return output
	*/
}

func main() {
	bytes := getFileContents("testFile.txt")
	wordCount := getWordCountFromBytes(bytes)
	fmt.Println(prettyPrint(wordCount))
}