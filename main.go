package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Global counters for directories and files
var dirCount, fileCount int
var mu sync.Mutex // Mutex to protect shared variables

// createHardLink creates a hard link for a file from src to dst.
func createHardLink(src, dst string) error {
	// Create the hard link at the destination
	err := os.Link(src, dst)
	if err != nil {
		return fmt.Errorf("failed to create hard link from %s to %s: %v", src, dst, err)
	}

	// Increment file count
	mu.Lock()
	fileCount++
	mu.Unlock()

	return nil
}

// worker listens to the file channel and creates hard links.
func worker(fileChan <-chan [2]string, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for paths := range fileChan {
		srcPath, dstPath := paths[0], paths[1]
		if err := createHardLink(srcPath, dstPath); err != nil {
			errChan <- err
		}
	}
}

// duplicateDirectoryStructure duplicates a directory tree with hard links for files using a worker pool.
func duplicateDirectoryStructure(srcRoot, dstRoot string, fileChan chan<- [2]string, errChan chan<- error) error {
	// Walk the source directory structure
	err := filepath.WalkDir(srcRoot, func(srcPath string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Compute the destination path
		relPath, err := filepath.Rel(srcRoot, srcPath)
		if err != nil {
			return fmt.Errorf("failed to compute relative path: %v", err)
		}
		dstPath := filepath.Join(dstRoot, relPath)

		// If it's a directory, create it in the destination
		if d.IsDir() {
			err := os.MkdirAll(dstPath, d.Type().Perm())
			if err != nil {
				return fmt.Errorf("failed to create directory %s: %v", dstPath, err)
			}

			// Increment directory count
			mu.Lock()
			dirCount++
			mu.Unlock()
		} else {
			// If it's a file, send the file paths to the channel for the worker pool
			fileChan <- [2]string{srcPath, dstPath}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// logErrors writes all errors to the given log file.
func logErrors(errChan chan error, logFile string) {
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return
	}
	defer f.Close()
	logger := log.New(f, "", log.LstdFlags)

	// Log each error received from the channel
	for err := range errChan {
		logger.Println(err)
	}
}

// progressReporter prints progress every 2 seconds.
func progressReporter(done chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mu.Lock()
			fmt.Printf("Progress: %d directories and %d files created\n", dirCount, fileCount)
			mu.Unlock()
		case <-done:
			return
		}
	}
}

func main() {
	// Parse source and destination directories from command-line arguments
	srcDir := flag.String("src", "", "Source directory")
	dstDir := flag.String("dst", "", "Destination directory")
	logFile := flag.String("log", "errors.log", "Log file for errors")
	numWorkers := flag.Int("workers", 10, "Number of worker goroutines")
	flag.Parse()

	if *srcDir == "" || *dstDir == "" {
		fmt.Println("Usage: program -src <source directory> -dst <destination directory> [-workers N] [-log file]")
		os.Exit(1)
	}

	// Check if the source directory exists
	if _, err := os.Stat(*srcDir); os.IsNotExist(err) {
		fmt.Printf("Source directory %s does not exist.\n", *srcDir)
		os.Exit(1)
	}

	// Channels for progress and error logging
	fileChan := make(chan [2]string, 100) // Buffered channel for file paths
	errChan := make(chan error, 1)
	done := make(chan struct{})

	// Start the progress reporter
	go progressReporter(done)

	// Start logging errors to a file
	go logErrors(errChan, *logFile)

	// Worker pool to create hard links
	var wg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go worker(fileChan, errChan, &wg)
	}

	// Start duplicating the directory structure with hard links
	fmt.Printf("Duplicating directory structure from %s to %s...\n", *srcDir, *dstDir)
	err := duplicateDirectoryStructure(*srcDir, *dstDir, fileChan, errChan)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		close(fileChan)
		os.Exit(1)
	}

	// Close the file channel to signal workers to exit
	close(fileChan)

	// Wait for all workers to finish processing
	wg.Wait()

	// Close the progress reporter
	done <- struct{}{}

	// Print final results
	mu.Lock()
	fmt.Printf("Done! Created %d directories and %d files.\n", dirCount, fileCount)
	mu.Unlock()

	// Allow error logging to complete before exit
	time.Sleep(1 * time.Second)
}
