package cerevaluation

import (
	"bufio"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	UnihanUnicodeVersion      = "17.0.0"
	UnihanProperty            = "kMandarin"
	UnihanSourceURL           = "https://www.unicode.org/Public/17.0.0/ucd/Unihan.zip"
	UnihanSourceArchiveSHA256 = "f7a48b2b545acfaa77b2d607ae28747404ce02baefee16396c5d2d7a8ef34b5e"
	UnihanSubsetFile          = "data/Unihan_kMandarin-17.0.0.txt"
	UnihanSubsetFileSHA256    = "07892add965a30e0a67b5003b8c08cd8d50a4c12f56c2ffab7ccd6c15464df70"
)

//go:embed data/Unihan_kMandarin-17.0.0.txt
var unihanFiles embed.FS

func loadMandarinReadings() (map[rune][]string, DataEvidence, error) {
	data, err := unihanFiles.ReadFile(UnihanSubsetFile)
	if err != nil {
		return nil, DataEvidence{}, fmt.Errorf("read embedded Unihan kMandarin data: %w", err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if digest != UnihanSubsetFileSHA256 {
		return nil, DataEvidence{}, fmt.Errorf("Unihan kMandarin data SHA-256 is %s, expected %s", digest, UnihanSubsetFileSHA256)
	}
	readings := make(map[rune][]string)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 || fields[1] != UnihanProperty || !strings.HasPrefix(fields[0], "U+") {
			return nil, DataEvidence{}, fmt.Errorf("invalid Unihan kMandarin row %q", line)
		}
		codePoint, parseErr := strconv.ParseInt(strings.TrimPrefix(fields[0], "U+"), 16, 32)
		if parseErr != nil {
			return nil, DataEvidence{}, fmt.Errorf("parse Unihan code point %q: %w", fields[0], parseErr)
		}
		char := rune(codePoint)
		if _, duplicate := readings[char]; duplicate {
			return nil, DataEvidence{}, fmt.Errorf("duplicate Unihan kMandarin entry for %s", fields[0])
		}
		values := strings.Fields(fields[2])
		if len(values) == 0 {
			return nil, DataEvidence{}, errors.New("Unihan kMandarin reading must be non-empty")
		}
		readings[char] = values
	}
	if err := scanner.Err(); err != nil {
		return nil, DataEvidence{}, err
	}
	if len(readings) == 0 {
		return nil, DataEvidence{}, errors.New("Unihan kMandarin data is empty")
	}
	return readings, DataEvidence{
		UnicodeVersion: UnihanUnicodeVersion, Property: UnihanProperty,
		SourceURL: UnihanSourceURL, SourceArchiveSHA256: UnihanSourceArchiveSHA256,
		SubsetFile: UnihanSubsetFile, SubsetFileSHA256: digest, EntryCount: len(readings),
	}, nil
}
