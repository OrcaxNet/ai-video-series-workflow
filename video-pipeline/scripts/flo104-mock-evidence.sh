#!/usr/bin/env sh
set -eu

output_root=${1:-artifacts/flo104-mock}

for tool in ffmpeg ffprobe; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "${tool} is required" >&2
    exit 1
  fi
done

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

mkdir -p "${output_root}"
ffmpeg_version=$(ffmpeg -version 2>/dev/null | sed -n '1p')
ffprobe_version=$(ffprobe -version 2>/dev/null | sed -n '1p')
burn_subtitles=false
if ffmpeg -hide_banner -filters 2>/dev/null | grep -q ' subtitles '; then
  burn_subtitles=true
fi

run_index=1
while [ "${run_index}" -le 3 ]; do
  case "${run_index}" in
    1)
      background_color=0x24435b
      tone_frequency=440
      ;;
    2)
      background_color=0x5b3424
      tone_frequency=523
      ;;
    *)
      background_color=0x315b24
      tone_frequency=659
      ;;
  esac

  run_dir="${output_root}/mock-run-${run_index}"
  mkdir -p "${run_dir}"

  printf '%s\n' \
    '1' \
    '00:00:02,000 --> 00:00:08,000' \
    "FLO-104 deterministic mock run ${run_index}." \
    '' \
    '2' \
    '00:00:18,000 --> 00:00:25,000' \
    'This subtitle is UTF-8 and externally deliverable.' \
    '' \
    '3' \
    '00:00:36,000 --> 00:00:44,000' \
    'Live provider quality remains pending_key.' \
    '' >"${run_dir}/subtitles.srt"

  {
    echo "${ffmpeg_version}"
    echo "evidence=mock_only"
    echo "burnSubtitles=${burn_subtitles}"
  } >"${run_dir}/ffmpeg.log"

  ffmpeg -nostdin -hide_banner -loglevel error -y \
    -f lavfi -i "color=c=${background_color}:s=1280x720:r=24:d=48" \
    -an -map_metadata -1 -fflags +bitexact \
    -c:v libx264 -preset medium -crf 18 -threads 1 -flags:v +bitexact \
    -pix_fmt yuv420p "${run_dir}/picture.mp4" \
    >>"${run_dir}/ffmpeg.log" 2>&1

  ffmpeg -nostdin -hide_banner -loglevel error -y \
    -f lavfi -i "sine=frequency=${tone_frequency}:sample_rate=48000:duration=48" \
    -af "volume=0.08,apad=whole_dur=48" -t 48 \
    -map_metadata -1 -fflags +bitexact -flags:a +bitexact \
    -c:a pcm_s16le "${run_dir}/dialogue.wav" \
    >>"${run_dir}/ffmpeg.log" 2>&1

  if [ "${burn_subtitles}" = true ]; then
    (
      cd "${run_dir}"
      ffmpeg -nostdin -hide_banner -loglevel error -y \
        -i picture.mp4 -i dialogue.wav \
        -filter_complex \
        "[0:v]subtitles=filename=subtitles.srt:force_style='FontSize=28,Outline=2,Shadow=0'[video];[1:a]loudnorm=I=-16:LRA=11:TP=-1.5,apad=whole_dur=48,asplit=2[program][dialogue]" \
        -map "[video]" -map "[program]" -map "[dialogue]" -t 48 \
        -map_metadata -1 -metadata "comment=AI-generated content" \
        -metadata:s:a:0 "title=Program Mix" \
        -metadata:s:a:1 "title=Dialogue" \
        -disposition:a:0 default -disposition:a:1 0 \
        -c:v libx264 -preset medium -crf 18 -threads 1 -flags:v +bitexact \
        -pix_fmt yuv420p -c:a aac -ar 48000 -ac 2 -movflags +faststart \
        sample.mp4
    ) >>"${run_dir}/ffmpeg.log" 2>&1
  else
    ffmpeg -nostdin -hide_banner -loglevel error -y \
      -i "${run_dir}/picture.mp4" -i "${run_dir}/dialogue.wav" \
      -filter_complex \
      "[1:a]loudnorm=I=-16:LRA=11:TP=-1.5,apad=whole_dur=48,asplit=2[program][dialogue]" \
      -map 0:v -map "[program]" -map "[dialogue]" -t 48 \
      -map_metadata -1 -metadata "comment=AI-generated content" \
      -metadata:s:a:0 "title=Program Mix" \
      -metadata:s:a:1 "title=Dialogue" \
      -disposition:a:0 default -disposition:a:1 0 \
      -c:v copy -c:a aac -ar 48000 -ac 2 -movflags +faststart \
      "${run_dir}/sample.mp4" \
      >>"${run_dir}/ffmpeg.log" 2>&1
  fi

  ffprobe -v error \
    -show_entries "format=duration:stream=index,codec_type,width,height,r_frame_rate,sample_rate,channels" \
    -of json "${run_dir}/sample.mp4" >"${run_dir}/ffprobe.json"

  video_hash=$(sha256_file "${run_dir}/sample.mp4")
  subtitle_hash=$(sha256_file "${run_dir}/subtitles.srt")
  dialogue_hash=$(sha256_file "${run_dir}/dialogue.wav")

  cat >"${run_dir}/input.json" <<EOF
{"schemaVersion":"v1","evidence":"mock_only","fixtureRun":${run_index},"durationMillis":48000,"output":{"width":1280,"height":720,"fps":24,"format":"mp4","burnSubtitles":${burn_subtitles},"externalSrt":true,"audioSampleRate":48000,"audioChannels":2},"providerCalls":0}
EOF

  cat >"${run_dir}/approvals.json" <<EOF
{"schemaVersion":"v1","evidence":"mock_only","gates":[{"gate":"G1","decision":"APPROVED_FIXTURE"},{"gate":"G2","decision":"APPROVED_FIXTURE"}],"g3":"NOT_OPENED_FOR_STANDALONE_FIXTURE"}
EOF

  cat >"${run_dir}/qc.json" <<EOF
{"schemaVersion":"v1","evidence":"mock_only","state":"STRUCTURAL_PASSED","actualDurationMillis":48000,"width":1280,"height":720,"fps":24,"subtitleCerPercent":null,"subtitleBoundaryP95Millis":null,"audioVideoStartP95Millis":null,"manualTimingRequired":true}
EOF

  cat >"${run_dir}/usage-cost.json" <<EOF
{"schemaVersion":"v1","evidence":"mock_only","providerCalls":0,"usage":null,"actualCost":null,"liveMetrics":"pending_key"}
EOF

  cat >"${run_dir}/service-bom.json" <<EOF
{"schemaVersion":"v1","evidence":"mock_only","components":[{"name":"ffmpeg","kind":"media-tool","version":"${ffmpeg_version}"},{"name":"ffprobe","kind":"media-tool","version":"${ffprobe_version}"},{"name":"fixture-tone","kind":"non-provider-test-source","version":"run-${run_index}"}]}
EOF

  cat >"${run_dir}/manifest.json" <<EOF
{"schemaVersion":"v1","evidence":"mock_only","fixtureRun":${run_index},"aiContentMarker":true,"outputs":[{"kind":"final_video","path":"sample.mp4","sha256":"${video_hash}","mediaType":"video/mp4"},{"kind":"subtitle_srt","path":"subtitles.srt","sha256":"${subtitle_hash}","mediaType":"application/x-subrip; charset=utf-8"},{"kind":"dialogue_audio","path":"dialogue.wav","sha256":"${dialogue_hash}","mediaType":"audio/wav"}],"qualityMeasurements":"pending_key","providerUsageAndCost":"not_applicable"}
EOF

  (
    cd "${run_dir}"
    for artifact in \
      sample.mp4 subtitles.srt dialogue.wav input.json approvals.json \
      qc.json usage-cost.json service-bom.json manifest.json ffprobe.json ffmpeg.log; do
      if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "${artifact}"
      else
        shasum -a 256 "${artifact}"
      fi
    done >SHA256SUMS
  )

  run_index=$((run_index + 1))
done

echo "Created three mock_only FLO-104 evidence packs under ${output_root}"
echo "No provider API or credential was used; live quality, latency, success, usage, and cost remain pending_key."
