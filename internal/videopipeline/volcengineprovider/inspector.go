package volcengineprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

// MediaSpec is measured from the downloaded object, never inferred from its
// filename or trusted solely from provider metadata.
type MediaSpec struct {
	Width          int
	Height         int
	FPS            int
	DurationMillis int64
	Format         string
}

type MediaInspector interface {
	Inspect(context.Context, string) (MediaSpec, error)
}

// FFprobeInspector measures the immutable local object committed to CAS.
type FFprobeInspector struct {
	Binary string
}

func (i FFprobeInspector) Inspect(ctx context.Context, path string) (MediaSpec, error) {
	binary := strings.TrimSpace(i.Binary)
	if binary == "" {
		binary = "ffprobe"
	}
	command := exec.CommandContext(ctx, binary,
		"-v", "error",
		"-show_entries", "stream=codec_type,width,height,avg_frame_rate,r_frame_rate:format=duration,format_name",
		"-of", "json",
		path,
	)
	output, err := command.Output()
	if err != nil {
		return MediaSpec{}, errors.New("ffprobe could not inspect the downloaded provider artifact")
	}
	var probe struct {
		Streams []struct {
			CodecType    string `json:"codec_type"`
			Width        int    `json:"width"`
			Height       int    `json:"height"`
			AverageRate  string `json:"avg_frame_rate"`
			ReportedRate string `json:"r_frame_rate"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
			Name     string `json:"format_name"`
		} `json:"format"`
	}
	if err := json.Unmarshal(output, &probe); err != nil {
		return MediaSpec{}, errors.New("ffprobe returned invalid JSON")
	}
	durationSeconds, err := strconv.ParseFloat(probe.Format.Duration, 64)
	if err != nil || durationSeconds <= 0 {
		return MediaSpec{}, errors.New("ffprobe returned an invalid duration")
	}
	for _, stream := range probe.Streams {
		if stream.CodecType != "video" {
			continue
		}
		fps, err := parseFrameRate(stream.AverageRate)
		if err != nil {
			fps, err = parseFrameRate(stream.ReportedRate)
		}
		if err != nil || stream.Width <= 0 || stream.Height <= 0 {
			return MediaSpec{}, errors.New("ffprobe returned an invalid video stream")
		}
		format := strings.Split(probe.Format.Name, ",")[0]
		if format == "mov" {
			format = "mp4"
		}
		return MediaSpec{
			Width:          stream.Width,
			Height:         stream.Height,
			FPS:            fps,
			DurationMillis: int64(math.Round(durationSeconds * 1000)),
			Format:         format,
		}, nil
	}
	return MediaSpec{}, errors.New("ffprobe found no video stream")
}

func parseFrameRate(value string) (int, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid frame rate")
	}
	numerator, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, err
	}
	denominator, err := strconv.ParseFloat(parts[1], 64)
	if err != nil || denominator == 0 {
		return 0, fmt.Errorf("invalid frame rate")
	}
	fps := int(math.Round(numerator / denominator))
	if fps <= 0 {
		return 0, fmt.Errorf("invalid frame rate")
	}
	return fps, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
