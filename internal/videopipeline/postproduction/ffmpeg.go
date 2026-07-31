package postproduction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

type Command struct {
	Program string   `json:"program"`
	Args    []string `json:"args"`
}

type CommandPlan struct {
	SchemaVersion string    `json:"schemaVersion"`
	Commands      []Command `json:"commands"`
}

type RenderResult struct {
	Dialogue        Artifact
	FinalVideo      Artifact
	CommandPlanHash string
	QC              QCReport
	FFmpegVersion   string
	FFprobeVersion  string
}

type MediaProcessor interface {
	Render(context.Context, Request, []byte, []ProviderAttempt) (RenderResult, error)
}

type CommandRunner interface {
	Run(context.Context, string, string, ...string) ([]byte, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(
	ctx context.Context,
	directory string,
	program string,
	args ...string,
) ([]byte, error) {
	command := exec.CommandContext(ctx, program, args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", program, err, sanitizeToolOutput(output))
	}
	return output, nil
}

type FFmpegProcessor struct {
	FFmpeg  string
	FFprobe string
	Store   *artifactstore.Store
	Runner  CommandRunner
}

func NewFFmpegProcessor(store *artifactstore.Store) (*FFmpegProcessor, error) {
	if store == nil {
		return nil, errors.New("artifact store is required")
	}
	return &FFmpegProcessor{
		FFmpeg:  "ffmpeg",
		FFprobe: "ffprobe",
		Store:   store,
		Runner:  ExecRunner{},
	}, nil
}

func (p *FFmpegProcessor) Render(
	ctx context.Context,
	request Request,
	subtitleBytes []byte,
	attempts []ProviderAttempt,
) (RenderResult, error) {
	if err := request.Validate(); err != nil {
		return RenderResult{}, err
	}
	if p.Store == nil || p.Runner == nil || p.FFmpeg == "" || p.FFprobe == "" {
		return RenderResult{}, errors.New("FFmpeg processor is not configured")
	}
	if len(attempts) != len(request.Subtitle.Cues) {
		return RenderResult{}, errors.New("one speech attempt is required for every subtitle cue")
	}
	workdir, err := os.MkdirTemp("", "video-postproduction-*")
	if err != nil {
		return RenderResult{}, fmt.Errorf("create post-production workspace: %w", err)
	}
	defer os.RemoveAll(workdir)

	if err := os.WriteFile(filepath.Join(workdir, "subtitles.srt"), subtitleBytes, 0o600); err != nil {
		return RenderResult{}, fmt.Errorf("write subtitle workspace file: %w", err)
	}
	// A mock Provider intentionally returns contract fixtures rather than
	// playable media. In mock_only mode the command plan creates deterministic
	// lavfi sources and preserves the Provider artifacts only as lineage. Live
	// evidence must always materialize and decode the actual Provider bytes.
	if request.Evidence != EvidenceMockOnly {
		for index, clip := range request.Clips {
			name := fmt.Sprintf("clip-%03d%s", index, extensionFor(clip.Artifact.MediaType))
			if err := p.materialize(clip.Artifact.Digest, filepath.Join(workdir, name)); err != nil {
				return RenderResult{}, err
			}
		}
		for index, attempt := range attempts {
			name := fmt.Sprintf("cue-%03d%s", index, extensionFor(attempt.Artifact.MediaType))
			if err := p.materialize(attempt.Artifact.Digest, filepath.Join(workdir, name)); err != nil {
				return RenderResult{}, err
			}
		}
	}
	if request.BackgroundAudio != nil {
		name := "background" + extensionFor(request.BackgroundAudio.MediaType)
		if err := p.materialize(request.BackgroundAudio.Digest, filepath.Join(workdir, name)); err != nil {
			return RenderResult{}, err
		}
	}

	plan, err := BuildCommandPlan(request, attempts, p.FFmpeg)
	if err != nil {
		return RenderResult{}, err
	}
	planHash, err := digestJSON(plan)
	if err != nil {
		return RenderResult{}, fmt.Errorf("hash FFmpeg command plan: %w", err)
	}
	if err := writeConcatList(workdir, len(request.Clips)); err != nil {
		return RenderResult{}, err
	}
	for _, command := range plan.Commands {
		if _, err := p.Runner.Run(ctx, workdir, command.Program, command.Args...); err != nil {
			return RenderResult{}, err
		}
	}
	probeOutput, err := p.Runner.Run(
		ctx,
		workdir,
		p.FFprobe,
		"-v", "error",
		"-show_entries", "format=duration:stream=codec_type,width,height,r_frame_rate,start_time,sample_rate,channels",
		"-of", "json",
		"episode.mp4",
	)
	if err != nil {
		return RenderResult{}, err
	}
	probe, err := parseProbe(probeOutput)
	if err != nil {
		return RenderResult{}, err
	}
	policy := request.Output.withDefaults()
	if probe.Width != policy.Width || probe.Height != policy.Height || probe.FPS != policy.FPS {
		return RenderResult{}, fmt.Errorf(
			"FFmpeg output is %dx%d@%dfps, expected %dx%d@%dfps",
			probe.Width, probe.Height, probe.FPS, policy.Width, policy.Height, policy.FPS,
		)
	}
	if probe.AudioStreams != 2 ||
		probe.AudioSampleRate != policy.AudioSampleRate ||
		probe.AudioChannels != policy.AudioChannels {
		return RenderResult{}, fmt.Errorf(
			"FFmpeg output audio is %d tracks at %d Hz/%d channels, expected 2 tracks at %d Hz/%d channels",
			probe.AudioStreams, probe.AudioSampleRate, probe.AudioChannels,
			policy.AudioSampleRate, policy.AudioChannels,
		)
	}
	if probe.AudioVideoStartDeltaMillis > 40 {
		return RenderResult{}, fmt.Errorf(
			"FFmpeg audio/video stream start delta is %dms, expected no more than 40ms",
			probe.AudioVideoStartDeltaMillis,
		)
	}
	expectedDuration := request.DurationMillis()
	if absolute(probe.DurationMillis-expectedDuration) > 40 {
		return RenderResult{}, fmt.Errorf(
			"FFmpeg output duration %dms differs from expected %dms",
			probe.DurationMillis, expectedDuration,
		)
	}
	dialogue, err := p.commitFile(ctx, filepath.Join(workdir, "dialogue.wav"), Artifact{
		Kind: "dialogue_audio", MediaType: "audio/wav",
		DurationMillis: probe.DurationMillis,
	})
	if err != nil {
		return RenderResult{}, err
	}
	finalVideo, err := p.commitFile(ctx, filepath.Join(workdir, "episode.mp4"), Artifact{
		Kind: "final_video", MediaType: "video/mp4",
		DurationMillis: probe.DurationMillis, Width: probe.Width, Height: probe.Height, FPS: probe.FPS,
	})
	if err != nil {
		return RenderResult{}, err
	}
	ffmpegVersion, err := p.toolVersion(ctx, workdir, p.FFmpeg)
	if err != nil {
		return RenderResult{}, err
	}
	ffprobeVersion, err := p.toolVersion(ctx, workdir, p.FFprobe)
	if err != nil {
		return RenderResult{}, err
	}
	return RenderResult{
		Dialogue:        dialogue,
		FinalVideo:      finalVideo,
		CommandPlanHash: planHash,
		QC: QCReport{
			State:                "STRUCTURAL_PASSED",
			ActualDurationMillis: probe.DurationMillis,
			// Structural probing cannot establish CER or timing p95. Keep the
			// manual timing gate closed until a measured live evidence report
			// supplies those metrics.
			ManualTimingRequired: true,
			MeasurementEvidence:  request.Evidence,
		},
		FFmpegVersion:  ffmpegVersion,
		FFprobeVersion: ffprobeVersion,
	}, nil
}

func BuildCommandPlan(
	request Request,
	attempts []ProviderAttempt,
	ffmpeg string,
) (CommandPlan, error) {
	if err := request.Validate(); err != nil {
		return CommandPlan{}, err
	}
	if len(attempts) != len(request.Subtitle.Cues) {
		return CommandPlan{}, errors.New("speech attempt count must match subtitle cue count")
	}
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	policy := request.Output.withDefaults()
	commands := make([]Command, 0, len(request.Clips)+3)
	for index, clip := range request.Clips {
		duration := seconds(clip.DurationMillis)
		inputName := fmt.Sprintf("clip-%03d%s", index, extensionFor(clip.Artifact.MediaType))
		outputName := fmt.Sprintf("segment-%03d.mkv", index)
		filter := fmt.Sprintf(
			"[0:v]scale=%d:%d:force_original_aspect_ratio=decrease,"+
				"pad=%d:%d:(ow-iw)/2:(oh-ih)/2,fps=%d,format=yuv420p,"+
				"trim=duration=%s,setpts=PTS-STARTPTS[v];"+
				"[1:a]atrim=duration=%s,asetpts=PTS-STARTPTS[a]",
			policy.Width, policy.Height, policy.Width, policy.Height, policy.FPS, duration, duration,
		)
		args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
		if request.Evidence == EvidenceMockOnly {
			args = append(args,
				"-f", "lavfi", "-i", fmt.Sprintf(
					"color=c=0x%s:s=%dx%d:r=%d:d=%s",
					clip.Artifact.Digest[:6], policy.Width, policy.Height, policy.FPS, duration,
				),
			)
		} else {
			args = append(args, "-i", inputName)
		}
		args = append(args,
			"-f", "lavfi", "-t", duration, "-i",
			fmt.Sprintf("anullsrc=channel_layout=stereo:sample_rate=%d", policy.AudioSampleRate),
			"-filter_complex", filter,
			"-map", "[v]", "-map", "[a]", "-t", duration,
			"-map_metadata", "-1", "-fflags", "+bitexact",
			"-c:v", "libx264", "-preset", "medium", "-crf", "18",
			"-threads", "1", "-flags:v", "+bitexact",
			"-c:a", "pcm_s16le", "-flags:a", "+bitexact",
			outputName,
		)
		commands = append(commands, Command{Program: ffmpeg, Args: args})
	}
	commands = append(commands, Command{
		Program: ffmpeg,
		Args: []string{
			"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
			"-f", "concat", "-safe", "1", "-i", "segments.txt",
			"-map_metadata", "-1", "-c", "copy", "shots.mkv",
		},
	})
	commands = append(commands, dialogueCommand(request, attempts, ffmpeg))
	commands = append(commands, finalCommand(request, ffmpeg))
	return CommandPlan{SchemaVersion: SchemaVersion, Commands: commands}, nil
}

func dialogueCommand(request Request, attempts []ProviderAttempt, ffmpeg string) Command {
	duration := seconds(request.DurationMillis())
	policy := request.Output.withDefaults()
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
	if len(attempts) == 0 {
		args = append(args,
			"-f", "lavfi", "-i",
			fmt.Sprintf("anullsrc=channel_layout=stereo:sample_rate=%d", policy.AudioSampleRate),
			"-t", duration, "-map_metadata", "-1", "-c:a", "pcm_s16le",
			"-fflags", "+bitexact", "-flags:a", "+bitexact", "dialogue.wav",
		)
		return Command{Program: ffmpeg, Args: args}
	}
	filterParts := make([]string, 0, len(attempts)+1)
	mixInputs := make([]string, 0, len(attempts))
	for index, attempt := range attempts {
		cue := request.Subtitle.Cues[index]
		if request.Evidence == EvidenceMockOnly {
			args = append(args,
				"-f", "lavfi", "-i", fmt.Sprintf(
					"sine=frequency=%d:sample_rate=%d:duration=%s",
					440+(index%8)*55,
					policy.AudioSampleRate,
					seconds(cue.EndMillis-cue.StartMillis),
				),
			)
		} else {
			args = append(args, "-i",
				fmt.Sprintf("cue-%03d%s", index, extensionFor(attempt.Artifact.MediaType)),
			)
		}
		label := fmt.Sprintf("cue%d", index)
		filterParts = append(filterParts, fmt.Sprintf(
			"[%d:a]aresample=%d,atrim=duration=%s,asetpts=PTS-STARTPTS,"+
				"adelay=%d|%d[%s]",
			index, policy.AudioSampleRate, seconds(cue.EndMillis-cue.StartMillis),
			cue.StartMillis, cue.StartMillis, label,
		))
		mixInputs = append(mixInputs, "["+label+"]")
	}
	filterParts = append(filterParts, fmt.Sprintf(
		"%samix=inputs=%d:normalize=0:duration=longest,"+
			"loudnorm=I=-16:LRA=11:TP=-1.5,apad=whole_dur=%s[dialogue]",
		strings.Join(mixInputs, ""), len(mixInputs), duration,
	))
	args = append(args,
		"-filter_complex", strings.Join(filterParts, ";"),
		"-map", "[dialogue]", "-t", duration, "-map_metadata", "-1",
		"-c:a", "pcm_s16le", "-fflags", "+bitexact", "-flags:a", "+bitexact",
		"dialogue.wav",
	)
	return Command{Program: ffmpeg, Args: args}
}

func finalCommand(request Request, ffmpeg string) Command {
	policy := request.Output.withDefaults()
	duration := seconds(request.DurationMillis())
	args := []string{
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-i", "shots.mkv", "-i", "dialogue.wav",
	}
	backgroundIndex := -1
	if request.BackgroundAudio != nil {
		backgroundIndex = 2
		args = append(args, "-stream_loop", "-1", "-i", "background"+extensionFor(request.BackgroundAudio.MediaType))
	}
	filterParts := make([]string, 0, 3)
	videoMap := "0:v"
	if policy.BurnSubtitles {
		filterParts = append(filterParts,
			"[0:v]subtitles=subtitles.srt:force_style='FontSize=28,Outline=2,Shadow=0'[video]",
		)
		videoMap = "[video]"
	}
	if backgroundIndex >= 0 {
		filterParts = append(filterParts,
			fmt.Sprintf("[%d:a]volume=0.18,atrim=duration=%s[background]", backgroundIndex, duration),
			"[0:a][1:a][background]amix=inputs=3:normalize=0:duration=first[program]",
		)
	} else {
		filterParts = append(filterParts,
			"[0:a][1:a]amix=inputs=2:normalize=0:duration=first[program]",
		)
	}
	args = append(args,
		"-filter_complex", strings.Join(filterParts, ";"),
		"-map", videoMap, "-map", "[program]", "-map", "1:a",
		"-t", duration,
		"-map_metadata", "-1",
		"-metadata", "comment=AI-generated content",
		"-metadata:s:a:0", "title=Program Mix",
		"-metadata:s:a:1", "title=Dialogue",
		"-disposition:a:0", "default",
		"-disposition:a:1", "0",
		"-c:v", "libx264", "-preset", "medium", "-crf", "18",
		"-pix_fmt", "yuv420p", "-r", strconv.Itoa(policy.FPS),
		"-threads", "1", "-flags:v", "+bitexact",
		"-c:a", "aac", "-ar", strconv.Itoa(policy.AudioSampleRate),
		"-ac", strconv.Itoa(policy.AudioChannels),
		"-movflags", "+faststart", "episode.mp4",
	)
	return Command{Program: ffmpeg, Args: args}
}

func (p *FFmpegProcessor) materialize(digest, destination string) error {
	source, err := p.Store.Open(digest)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create materialized artifact: %w", err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		return fmt.Errorf("materialize artifact: %w", err)
	}
	if err := target.Close(); err != nil {
		return fmt.Errorf("close materialized artifact: %w", err)
	}
	return nil
}

func (p *FFmpegProcessor) commitFile(
	ctx context.Context,
	path string,
	template Artifact,
) (Artifact, error) {
	file, err := os.Open(path)
	if err != nil {
		return Artifact{}, fmt.Errorf("open FFmpeg output: %w", err)
	}
	defer file.Close()
	committed, err := p.Store.Put(ctx, file)
	if err != nil {
		return Artifact{}, fmt.Errorf("commit FFmpeg output: %w", err)
	}
	template.Digest = committed.Digest
	template.URI = committed.URI
	template.SizeBytes = committed.Size
	if err := template.Validate(); err != nil {
		return Artifact{}, err
	}
	return template, nil
}

func (p *FFmpegProcessor) toolVersion(
	ctx context.Context,
	workdir string,
	program string,
) (string, error) {
	output, err := p.Runner.Run(ctx, workdir, program, "-version")
	if err != nil {
		return "", err
	}
	line, _, _ := strings.Cut(string(output), "\n")
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("%s returned an empty version", program)
	}
	return line, nil
}

type probeResult struct {
	DurationMillis             int64
	Width                      int
	Height                     int
	FPS                        int
	AudioStreams               int
	AudioSampleRate            int
	AudioChannels              int
	AudioVideoStartDeltaMillis int64
}

func parseProbe(data []byte) (probeResult, error) {
	var payload struct {
		Streams []struct {
			CodecType  string `json:"codec_type"`
			Width      int    `json:"width"`
			Height     int    `json:"height"`
			FrameRate  string `json:"r_frame_rate"`
			StartTime  string `json:"start_time"`
			SampleRate string `json:"sample_rate"`
			Channels   int    `json:"channels"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	// ffprobe adds top-level collections such as programs and stream_groups in
	// newer releases even when show_entries is constrained. Ignore additive
	// fields so probing remains portable across supported FFmpeg versions.
	if err := decoder.Decode(&payload); err != nil {
		return probeResult{}, fmt.Errorf("decode ffprobe output: %w", err)
	}
	durationSeconds, err := strconv.ParseFloat(payload.Format.Duration, 64)
	if err != nil || durationSeconds <= 0 {
		return probeResult{}, errors.New("ffprobe duration is invalid")
	}
	result := probeResult{DurationMillis: int64(durationSeconds*1000 + 0.5)}
	var videoStartMillis *int64
	audioStarts := make([]int64, 0, 2)
	for _, stream := range payload.Streams {
		startMillis, err := parseStartMillis(stream.StartTime)
		if err != nil {
			return probeResult{}, err
		}
		switch stream.CodecType {
		case "video":
			if result.Width > 0 {
				continue
			}
			result.Width, result.Height = stream.Width, stream.Height
			parts := strings.Split(stream.FrameRate, "/")
			if len(parts) != 2 {
				return probeResult{}, errors.New("ffprobe frame rate is invalid")
			}
			numerator, numeratorErr := strconv.Atoi(parts[0])
			denominator, denominatorErr := strconv.Atoi(parts[1])
			if numeratorErr != nil || denominatorErr != nil || denominator == 0 {
				return probeResult{}, errors.New("ffprobe frame rate is invalid")
			}
			result.FPS = int(float64(numerator)/float64(denominator) + 0.5)
			videoStartMillis = &startMillis
		case "audio":
			sampleRate, sampleRateErr := strconv.Atoi(stream.SampleRate)
			if sampleRateErr != nil || sampleRate <= 0 || stream.Channels <= 0 {
				return probeResult{}, errors.New("ffprobe audio stream specification is invalid")
			}
			if result.AudioStreams == 0 {
				result.AudioSampleRate = sampleRate
				result.AudioChannels = stream.Channels
			} else if result.AudioSampleRate != sampleRate ||
				result.AudioChannels != stream.Channels {
				return probeResult{}, errors.New("ffprobe audio streams have inconsistent specifications")
			}
			result.AudioStreams++
			audioStarts = append(audioStarts, startMillis)
		}
	}
	if result.Width <= 0 || result.Height <= 0 || result.FPS <= 0 {
		return probeResult{}, errors.New("ffprobe did not report a valid video stream")
	}
	if videoStartMillis == nil || result.AudioStreams == 0 {
		return probeResult{}, errors.New("ffprobe did not report required video and audio streams")
	}
	for _, audioStartMillis := range audioStarts {
		delta := absolute(audioStartMillis - *videoStartMillis)
		if delta > result.AudioVideoStartDeltaMillis {
			result.AudioVideoStartDeltaMillis = delta
		}
	}
	return result, nil
}

func parseStartMillis(value string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, errors.New("ffprobe stream start time is invalid")
	}
	return int64(seconds*1000 + 0.5), nil
}

func writeConcatList(workdir string, clips int) error {
	var content strings.Builder
	for index := range clips {
		content.WriteString(fmt.Sprintf("file 'segment-%03d.mkv'\n", index))
	}
	if err := os.WriteFile(filepath.Join(workdir, "segments.txt"), []byte(content.String()), 0o600); err != nil {
		return fmt.Errorf("write FFmpeg concat list: %w", err)
	}
	return nil
}

func seconds(milliseconds int64) string {
	return strconv.FormatFloat(float64(milliseconds)/1000, 'f', 3, 64)
}

func extensionFor(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "video/mp4":
		return ".mp4"
	case "video/webm":
		return ".webm"
	case "audio/mpeg":
		return ".mp3"
	case "audio/mp4":
		return ".m4a"
	case "audio/flac":
		return ".flac"
	case "audio/wav", "audio/x-wav":
		return ".wav"
	default:
		return ".bin"
	}
}

func sanitizeToolOutput(output []byte) string {
	const maximum = 2_000
	clean := strings.ReplaceAll(string(output), "\x00", "")
	if len(clean) > maximum {
		clean = clean[len(clean)-maximum:]
	}
	return strings.TrimSpace(clean)
}

func absolute(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
