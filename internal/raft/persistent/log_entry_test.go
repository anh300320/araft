package persistent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anh300320/araft/internal/raft/common"
)

func TestAppendEntry_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "log.output")

	storage := LogEntryFileStorage{DataFilePath: file}
	entry := common.LogEntry{
		Index: 99,
		Term:  1,
		Data:  "hello",
	}

	written, err := storage.AppendEntry(entry)
	if err != nil {
		t.Fatalf("AppendEntry() error = %v", err)
	}
	if written.Index != 1 {
		t.Fatalf("AppendEntry() index = %d, want 1", written.Index)
	}
	if written.Term != 1 {
		t.Fatalf("AppendEntry() term = %d, want 1", written.Term)
	}
	if written.Data != "hello" {
		t.Fatalf("AppendEntry() data = %q, want %q", written.Data, "hello")
	}

	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if got, want := string(content), "1|1|hello\n"; got != want {
		t.Fatalf("file content = %q, want %q", got, want)
	}
}

func TestAppendEntry_NonEmptyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "log.output")
	if err := os.WriteFile(file, []byte("3|2|existing\n"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	storage := LogEntryFileStorage{DataFilePath: file}
	entry := common.LogEntry{
		Term: 3,
		Data: "next",
	}

	written, err := storage.AppendEntry(entry)
	if err != nil {
		t.Fatalf("AppendEntry() error = %v", err)
	}
	if written.Index != 4 {
		t.Fatalf("AppendEntry() index = %d, want 4", written.Index)
	}
	if written.Term != 3 {
		t.Fatalf("AppendEntry() term = %d, want 3", written.Term)
	}
	if written.Data != "next" {
		t.Fatalf("AppendEntry() data = %q, want %q", written.Data, "next")
	}

	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if got, want := string(content), "3|2|existing\n4|3|next\n"; got != want {
		t.Fatalf("file content = %q, want %q", got, want)
	}
}

func TestAppendEntry_MultipleEntries(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "log.output")
	storage := LogEntryFileStorage{DataFilePath: file}

	entries := []common.LogEntry{
		{Term: 1, Data: "first"},
		{Term: 1, Data: "second"},
		{Term: 2, Data: "third"},
	}

	for i, entry := range entries {
		written, err := storage.AppendEntry(entry)
		if err != nil {
			t.Fatalf("AppendEntry(%d) error = %v", i, err)
		}
		wantIndex := common.LogIndex(i + 1)
		if written.Index != wantIndex {
			t.Fatalf("AppendEntry(%d) index = %d, want %d", i, written.Index, wantIndex)
		}
		if written.Term != entry.Term {
			t.Fatalf("AppendEntry(%d) term = %d, want %d", i, written.Term, entry.Term)
		}
		if written.Data != entry.Data {
			t.Fatalf("AppendEntry(%d) data = %q, want %q", i, written.Data, entry.Data)
		}
	}

	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	want := strings.Join([]string{
		"1|1|first",
		"2|1|second",
		"3|2|third",
	}, "\n") + "\n"
	if got := string(content); got != want {
		t.Fatalf("file content = %q, want %q", got, want)
	}
}

func TestAppendEntry_ExistingEmptyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "log.output")
	if err := os.WriteFile(file, nil, 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	storage := LogEntryFileStorage{DataFilePath: file}
	written, err := storage.AppendEntry(common.LogEntry{Term: 1, Data: "seed"})
	if err != nil {
		t.Fatalf("AppendEntry() error = %v", err)
	}
	if written.Index != 1 {
		t.Fatalf("AppendEntry() index = %d, want 1", written.Index)
	}
}

func TestAppendEntry_CorruptedLastLine(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name:    "wrong number of fields",
			content: "1|2\n",
		},
		{
			name:    "non-numeric index",
			content: "abc|1|data\n",
		},
		{
			name:    "non-numeric term",
			content: "1|abc|data\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			file := filepath.Join(dir, "log.output")
			if err := os.WriteFile(file, []byte(tt.content), 0644); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			storage := LogEntryFileStorage{DataFilePath: file}
			_, err := storage.AppendEntry(common.LogEntry{Term: 1, Data: "new"})
			if err == nil {
				t.Fatal("AppendEntry() error = nil, want error")
			}
			if !strings.Contains(err.Error(), "failed to load last entry") {
				t.Fatalf("AppendEntry() error = %q, want load last entry failure", err.Error())
			}
		})
	}
}

func TestParseLine(t *testing.T) {
	storage := LogEntryFileStorage{}

	tests := []struct {
		name    string
		line    string
		want    common.LogEntry
		wantErr bool
	}{
		{
			name: "valid line",
			line: "5|3|payload",
			want: common.LogEntry{Index: 5, Term: 3, Data: "payload"},
		},
		{
			name:    "too few fields",
			line:    "5|3",
			wantErr: true,
		},
		{
			name:    "too many fields",
			line:    "5|3|a|b",
			wantErr: true,
		},
		{
			name:    "invalid index",
			line:    "x|3|payload",
			wantErr: true,
		},
		{
			name:    "invalid term",
			line:    "5|x|payload",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := storage.parseLine(tt.line)
			if tt.wantErr {
				if err == nil {
					t.Fatal("parseLine() error = nil, want error")
				}
				if !strings.Contains(err.Error(), "corrupted data") {
					t.Fatalf("parseLine() error = %q, want corrupted data error", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLine() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseLine() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
