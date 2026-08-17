package persistent

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/anh300320/araft/internal/raft/common"
)

type LogEntryPersistent interface {
	AppendEntry(logEntry common.LogEntry) (common.LogEntry, error)
	GetLogEntriesStartAt(logIndex common.LogIndex, limit int) ([]common.LogEntry, error)
	GetLogEntriesFromTermBeginning(term common.Term, limit int) ([]common.LogEntry, error)
}

type LogEntryFileStorage struct {
	DataFilePath string
}

func (l *LogEntryFileStorage) AppendEntry(logEntry common.LogEntry) (common.LogEntry, error) {
	fileExists, err := common.FileExists(l.DataFilePath)
	if err != nil {
		return common.LogEntry{}, fmt.Errorf("failed to check if data file exists %w", err)
	}

	f, err := os.OpenFile(l.DataFilePath, os.O_RDWR|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return common.LogEntry{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return common.LogEntry{}, err
	}

	isEmpty := info.Size() == 0
	lastEntry := common.LogEntry{}
	if fileExists && !isEmpty {
		lastEntry, err = l.getLastEntry(f)
		if err != nil {
			return common.LogEntry{}, fmt.Errorf("failed to load last entry %w", err)
		}
	}

	logEntry.Index = lastEntry.Index + 1
	err = l.writeLogEntry(f, logEntry)
	return logEntry, err
}

func (l *LogEntryFileStorage) getLastEntry(f *os.File) (common.LogEntry, error) {
	lastLine, err := l.loadLastDataLine(f)
	if err != nil {
		return common.LogEntry{}, err
	}
	return l.parseLine(lastLine)
}

func (l *LogEntryFileStorage) parseLine(line string) (common.LogEntry, error) {
	cells := strings.Split(line, "|")
	if len(cells) != 3 {
		return common.LogEntry{}, fmt.Errorf("corrupted data, line: %s, cells = %s", line, cells)
	}
	index, err := strconv.Atoi(cells[0])
	if err != nil {
		return common.LogEntry{}, fmt.Errorf("corrupted data, line: %s, err %w", line, err)
	}
	term, err := strconv.Atoi(cells[1])
	if err != nil {
		return common.LogEntry{}, fmt.Errorf("corrupted data, line: %s, err %w", line, err)
	}
	data := cells[2]
	return common.LogEntry{
		Index: common.LogIndex(index),
		Term:  common.Term(term),
		Data:  data,
	}, nil
}

func (l *LogEntryFileStorage) writeLogEntry(f *os.File, logEntry common.LogEntry) error {
	_, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to go to the EOF to append %w", err)
	}
	dataLine := []string{strconv.FormatInt(logEntry.Index, 10), strconv.FormatInt(int64(logEntry.Term), 10), logEntry.Data}
	dataLineStr := strings.Join(dataLine, "|") + "\n"
	_, err = f.WriteString(dataLineStr)
	if err != nil {
		return fmt.Errorf("failed to write data line %w", err)
	}
	return nil
}

func (l *LogEntryFileStorage) loadLastDataLine(f *os.File) (string, error) {
	loadedLastLine := false
	bufferSize := 4 * 1024 // read a chunk of 4KB at a time
	buffer := make([]byte, bufferSize)
	lastLine := ""

	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get stat of err = %w", err)
	}
	fileSize := info.Size()
	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return "", fmt.Errorf("failed to seek end of file, err = %w", err)
	}
	readOffset := fileSize
	firstRead := true
	for !loadedLastLine && readOffset != 0 {
		step := -min(readOffset, int64(bufferSize))
		readOffset, err = f.Seek(int64(step), io.SeekCurrent)
		if err != nil {
			return "", fmt.Errorf("failed to seek step %d, err = %w", step, err)
		}
		readByteCnt, err := f.ReadAt(buffer, readOffset)
		if err != nil && err != io.EOF {
			return "", err
		}
		bufferRunes := []rune(string(buffer[:min(readByteCnt, len(buffer))]))

		occurence := 1
		if firstRead {
			occurence = 2
		}
		nextLineIndex := l.searchFromRightSide(bufferRunes, "\n", occurence)
		left := 0
		if nextLineIndex != -1 {
			left = nextLineIndex
			loadedLastLine = true
		}
		lastLine = string(bufferRunes[left:]) + lastLine
		firstRead = false
	}

	if lastLine == "" {
		return "", fmt.Errorf("failed to get the last line, unknown error")
	}
	return strings.TrimSpace(lastLine), nil
}

func (l *LogEntryFileStorage) searchFromRightSide(s []rune, ch string, occurence int) int {
	cnt := 0
	for i := len(s) - 1; i >= 0; i-- {
		if string(s[i]) == ch {
			cnt += 1
			if cnt == occurence {
				return i
			}
		}
	}
	return -1
}

func (l *LogEntryFileStorage) GetLogEntriesStartAt(logIndex common.LogIndex, limit int) ([]common.LogEntry, error) {
	result := make([]common.LogEntry, 0)
	return result, nil
}

func (l *LogEntryFileStorage) GetLogEntriesFromTermBeginning(term common.Term, limit int) ([]common.LogEntry, error) {
	result := make([]common.LogEntry, 0)
	return result, nil
}
