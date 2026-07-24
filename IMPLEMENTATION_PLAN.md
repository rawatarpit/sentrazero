# Implementation Plan: Universal File Processing & GPU Scheduling

## Overview

Three phases to make SentraZero's agent handle any file type (images, video, PDFs, audio, binaries), chunk them intelligently, and schedule GPU-bound plugins to GPU-capable devices.

| Phase | What | Where | Effort |
|-------|------|-------|--------|
| **1** | Rich Media Scan — extract metadata from any file | Go agent | 1-2 days |
| **2** | File/Byte-Range Chunking — chunk non-tabular files | Supabase + Go agent | 3-5 days |
| **3** | GPU-Aware Scheduling — match GPU plugins to GPU devices | Go agent + Supabase | 1-2 days |

---

## Phase 1: Rich Media Scan

**Goal:** `extractFileMetadata` returns real metadata (dimensions, duration, page count, codec) for images, video, PDFs, audio, archives, and binaries — instead of `format: "unknown"`.

### File: `internal/dispatcher/handlers_unix.go`

#### 1.1 `extractImageMetadata(path string) (map[string]any, error)`

```
Location: near line 1858 (after detectAndExtractMetadata)
Dependencies: stdlib image (JPEG, PNG, GIF), golang.org/x/image/webp (WebP)
```

```go
func extractImageMetadata(_ context.Context, filePath string) (map[string]any, error) {
    f, err := os.Open(filePath)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    meta := map[string]any{
        "format": "image",
    }

    // Detect actual format from header
    buf := make([]byte, 512)
    f.Read(buf)
    f.Seek(0, 0)
    mimeType := http.DetectContentType(buf)
    meta["image_mime"] = mimeType

    // Decode config for dimensions
    cfg, _, err := image.DecodeConfig(f)
    if err == nil {
        meta["image_width"] = cfg.Width
        meta["image_height"] = cfg.Height
        meta["image_color_model"] = cfg.ColorModel.String()
    }

    // File size
    fi, _ := os.Stat(filePath)
    meta["file_size_bytes"] = fi.Size()

    return meta, nil
    // Note: EXIF extraction (orientation, camera, GPS) can be added later
    // with github.com/rwcarlsen/goexif or github.com/dsoprea/go-exif
}
```

**Supported formats:** JPEG, PNG, GIF, WebP (with `golang.org/x/image/webp`), BMP, TIFF (stdlib)

#### 1.2 `extractVideoMetadata(path string) (map[string]any, error)`

```
Dependencies: none (pure Go reading of MP4/MKV headers for basic info)
```

```go
func extractVideoMetadata(_ context.Context, filePath string) (map[string]any, error) {
    meta := map[string]any{
        "format": "video",
    }

    f, err := os.Open(filePath)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    fi, _ := os.Stat(filePath)
    meta["file_size_bytes"] = fi.Size()

    // Read first 4KB for container detection
    buf := make([]byte, 4096)
    f.Read(buf)
    f.Seek(0, 0)

    switch {
    case bytes.HasPrefix(buf, []byte("ftyp")): // MP4 (ISOBMFF)
        meta["video_container"] = "mp4"
        // Parse ftyp box for major brand
        if len(buf) > 8 {
            meta["video_codec"] = string(bytes.TrimRight(buf[8:12], "\x00"))
        }
    case bytes.HasPrefix(buf, []byte{0x1a, 0x45, 0xdf, 0xa3}): // MKV/WebM
        meta["video_container"] = "matroska"
    case bytes.HasPrefix(buf, []byte("RIFF")) && bytes.Contains(buf[:12], []byte("AVI ")):
        meta["video_container"] = "avi"
    case bytes.HasPrefix(buf, []byte{0x00, 0x00, 0x00, 0x1c, 0x66, 0x74, 0x79, 0x70}):
        meta["video_container"] = "quicktime"
    default:
        meta["video_container"] = "unknown"
    }

    // Duration from container headers requires full parsing (future)
    // For now: file size as proxy
    meta["estimated_duration_seconds"] = float64(fi.Size()) / 500000 // rough: 500KB/s avg bitrate

    // Video quality heuristic from file size
    if fi.Size() > 500*1024*1024 {
        meta["quality_estimate"] = "high"
    } else if fi.Size() > 50*1024*1024 {
        meta["quality_estimate"] = "medium"
    } else {
        meta["quality_estimate"] = "low"
    }

    return meta, nil
    // Future: use ffprobe or go-astits/go-mp4 for accurate duration/codec/resolution
}
```

#### 1.3 `extractPDFMetadata(path string) (map[string]any, error)`

```
Dependencies: github.com/pdfcpu/pdfcpu
```

```go
func extractPDFMetadata(_ context.Context, filePath string) (map[string]any, error) {
    meta := map[string]any{
        "format": "pdf",
    }

    fi, _ := os.Stat(filePath)
    meta["file_size_bytes"] = fi.Size()

    // Use pdfcpu for page count
    conf := pdfcpu.NewDefaultConfiguration()
    conf.Cmd = pdfcpu.VALIDATE

    pdfInfo, err := pdfcpu.PDFInfo(filePath, conf)
    if err == nil {
        meta["pdf_page_count"] = pdfInfo.PageCount
        meta["pdf_version"] = pdfInfo.Version
        meta["pdf_encrypted"] = pdfInfo.IsEncrypted
        meta["pdf_linearized"] = pdfInfo.IsLinearized
    }

    return meta, nil
}
```

#### 1.4 `extractAudioMetadata(path string) (map[string]any, error)`

```
Dependencies: none (ID3v2 frame parsing, WAV/FLAC header parsing)
```

```go
func extractAudioMetadata(_ context.Context, filePath string) (map[string]any, error) {
    meta := map[string]any{
        "format": "audio",
    }

    f, err := os.Open(filePath)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    fi, _ := os.Stat(filePath)
    meta["file_size_bytes"] = fi.Size()

    buf := make([]byte, 4096)
    f.Read(buf)
    f.Seek(0, 0)

    switch {
    case bytes.HasPrefix(buf, []byte("ID3")): // MP3 with ID3v2
        meta["audio_codec"] = "mp3"
        // Parse ID3v2 header for sample rate
        if len(buf) > 10 {
            id3Size := int(buf[6])<<21 | int(buf[7])<<14 | int(buf[8])<<7 | int(buf[9])
            meta["id3_tag_present"] = true
            meta["id3_tag_size"] = id3Size
        }
    case bytes.HasPrefix(buf, []byte("RIFF")) && bytes.Contains(buf[:12], []byte("WAVE")):
        meta["audio_codec"] = "pcm"
        // WAV fmt chunk at offset 12
        if len(buf) > 28 {
            sampleRate := binary.LittleEndian.Uint32(buf[24:28])
            meta["audio_sample_rate"] = sampleRate
        }
    case bytes.HasPrefix(buf, []byte("fLaC")): // FLAC
        meta["audio_codec"] = "flac"
        // FLAC STREAMINFO block at offset 8
    case buf[0] == 0xFF && (buf[1]&0xE0) == 0xE0: // MP3 with sync header (no ID3)
        meta["audio_codec"] = "mp3"
        // MP3 frame header at offset 0
    default:
        meta["audio_codec"] = "unknown"
    }

    return meta, nil
}
```

#### 1.5 `extractArchiveMetadata(path string) (map[string]any, error)`

```
Dependencies: stdlib archive/zip, archive/tar, compress/gzip, compress/bzip2
```

```go
func extractArchiveMetadata(_ context.Context, filePath string) (map[string]any, error) {
    meta := map[string]any{
        "format": "archive",
    }

    fi, _ := os.Stat(filePath)
    meta["file_size_bytes"] = fi.Size()

    // Try ZIP
    if zr, err := zip.OpenReader(filePath); err == nil {
        meta["archive_format"] = "zip"
        meta["archive_file_count"] = len(zr.File)
        var totalSize int64
        for _, f := range zr.File {
            totalSize += int64(f.UncompressedSize64)
        }
        meta["archive_uncompressed_bytes"] = totalSize
        if fi.Size() > 0 {
            meta["archive_compression_ratio"] = float64(totalSize) / float64(fi.Size())
        }
        zr.Close()
        return meta, nil
    }

    // Try TAR (with optional compression)
    f, _ := os.Open(filePath)
    defer f.Close()

    var tr io.Reader = f
    gzBuf := make([]byte, 2)
    f.Read(gzBuf)
    f.Seek(0, 0)

    if gzBuf[0] == 0x1F && gzBuf[1] == 0x8B {
        meta["archive_format"] = "tar.gz"
        if gr, err := gzip.NewReader(f); err == nil {
            tr = gr
            defer gr.Close()
        }
    } else {
        meta["archive_format"] = "tar"
    }

    tarReader := tar.NewReader(tr)
    var fileCount int
    var totalSize int64
    for {
        hdr, err := tarReader.Next()
        if err == io.EOF {
            break
        }
        if err != nil {
            break
        }
        if hdr.Typeflag == tar.TypeReg {
            fileCount++
            totalSize += hdr.Size
        }
    }
    meta["archive_file_count"] = fileCount
    meta["archive_uncompressed_bytes"] = totalSize
    if fi.Size() > 0 {
        meta["archive_compression_ratio"] = float64(totalSize) / float64(fi.Size())
    }

    return meta, nil
}
```

#### 1.6 `extractBinaryMetadata(path string) (map[string]any, error)`

```
Dependencies: stdlib debug/elf, debug/pe, debug/macho
```

```go
func extractBinaryMetadata(_ context.Context, filePath string) (map[string]any, error) {
    meta := map[string]any{
        "format": "binary",
    }

    fi, _ := os.Stat(filePath)
    meta["file_size_bytes"] = fi.Size()

    // Try ELF
    if ef, err := elf.Open(filePath); err == nil {
        meta["binary_format"] = "elf"
        meta["binary_arch"] = ef.Machine.String()
        if ef.FileHeader.Type == elf.ET_EXEC {
            meta["binary_type"] = "executable"
        } else if ef.FileHeader.Type == elf.ET_DYN {
            meta["binary_type"] = "shared_library"
        } else if ef.FileHeader.Type == elf.ET_REL {
            meta["binary_type"] = "relocatable"
        }
        ef.Close()
        return meta, nil
    }

    // Try PE (Windows)
    if pef, err := pe.Open(filePath); err == nil {
        meta["binary_format"] = "pe"
        switch pef.Machine {
        case pe.IMAGE_FILE_MACHINE_I386:
            meta["binary_arch"] = "i386"
        case pe.IMAGE_FILE_MACHINE_AMD64:
            meta["binary_arch"] = "x86_64"
        case pe.IMAGE_FILE_MACHINE_ARM64:
            meta["binary_arch"] = "arm64"
        default:
            meta["binary_arch"] = pef.Machine.String()
        }
        if len(pef.Sections) > 0 {
            meta["binary_subsystem"] = pef.Subsystem(pef.Sections[0].Offset)
        }
        pef.Close()
        return meta, nil
    }

    // Try Mach-O (macOS)
    if mf, err := macho.Open(filePath); err == nil {
        meta["binary_format"] = "macho"
        meta["binary_arch"] = mf.Cpu.String()
        switch mf.Type {
        case macho.TypeExec:
            meta["binary_type"] = "executable"
        case macho.TypeDylib:
            meta["binary_type"] = "dynamic_library"
        case macho.TypeBundle:
            meta["binary_type"] = "bundle"
        }
        mf.Close()
        return meta, nil
    }

    meta["binary_format"] = "unknown"
    return meta, nil
}
```

#### 1.7 Update `extractFileMetadata` Switch

```go
// Current (line ~1540):
func extractFileMetadata(ctx context.Context, filePath string) (map[string]any, error) {
    ext := strings.ToLower(filepath.Ext(filePath))
    switch ext {
    case ".csv", ".tsv", ".txt":
        return extractCSVMetadata(ctx, filePath)
    case ".json", ".jsonl":
        return extractJSONMetadata(ctx, filePath)
    case ".parquet":
        return extractParquetMetadata(ctx, filePath)
    case ".ndjson":
        return extractJSONLMetadata(ctx, filePath)
    default:
        // New: try content-based detection with rich extractors
        return detectAndExtractMetadata(ctx, filePath)
    }
}
```

**New dispatch** — add before `default`:

```go
    // Image formats
    case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".svg":
        return extractImageMetadata(ctx, filePath)
    // Video formats
    case ".mp4", ".mkv", ".webm", ".avi", ".mov", ".wmv", ".flv", ".m4v":
        return extractVideoMetadata(ctx, filePath)
    // PDF
    case ".pdf":
        return extractPDFMetadata(ctx, filePath)
    // Audio formats
    case ".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a":
        return extractAudioMetadata(ctx, filePath)
    // Archive formats
    case ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar":
        return extractArchiveMetadata(ctx, filePath)
    // Binary formats
    case ".so", ".dll", ".dylib", ".exe", ".elf", ".o", ".obj":
        return extractBinaryMetadata(ctx, filePath)
```

#### 1.8 Update Scan Summary

The scan summary already uses a freeform `map[string]any` — new keys just appear. No struct changes needed.

#### 1.9 `go.mod` Dependencies

```go
require (
    // existing...
    golang.org/x/image v0.18.0      // for WebP decoding
    github.com/pdfcpu/pdfcpu v0.8.0 // for PDF metadata
)
```

---

## Phase 2: File/Byte-Range Chunking

**Goal:** When a dataset is a video, image collection, PDF collection, or archive, chunk by byte-range or file-per-chunk instead of row-range.

### 2.1 Database Schema — New Migration

**File:** `supabase/migrations/20260610000001_byte_range_chunking.sql`

```sql
-- ============================================================
-- Add chunk strategy to batch_chunks
-- ============================================================
ALTER TABLE batch_chunks ADD COLUMN chunk_strategy text NOT NULL DEFAULT 'row_range'
  CHECK (chunk_strategy IN ('row_range', 'byte_range', 'file_per_chunk', 'content_aware'));

ALTER TABLE batch_chunks ADD COLUMN byte_range_start bigint;
ALTER TABLE batch_chunks ADD COLUMN byte_range_end bigint;

-- file_list: array of {path, size, detected_format}
ALTER TABLE batch_chunks ADD COLUMN file_list jsonb DEFAULT '[]'::jsonb;

-- source_file: the source object key this byte range belongs to
ALTER TABLE batch_chunks ADD COLUMN source_file text;

-- ============================================================
-- Add file manifest to datasets (populated by scan)
-- ============================================================
ALTER TABLE datasets ADD COLUMN file_manifest jsonb DEFAULT '[]'::jsonb;
COMMENT ON COLUMN datasets.file_manifest IS
  'Array of {path, size, detected_format, md5} from scan. Used by pre_chunk_filebased for non-tabular chunking.';

-- ============================================================
-- Index for byte-range chunks
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_batch_chunks_strategy
  ON batch_chunks (dataset_id, chunk_strategy, status)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_batch_chunks_byte_range
  ON batch_chunks (dataset_id, source_file, byte_range_start)
  WHERE chunk_strategy = 'byte_range' AND status = 'pending';
```

### 2.2 `pre_chunk_filebased` RPC — Full Implementation

```sql
CREATE OR REPLACE FUNCTION public.pre_chunk_filebased(
    p_dataset_id uuid,
    p_org_id uuid,
    p_max_chunk_size_mb int DEFAULT 50,
    p_max_files_per_chunk int DEFAULT 50
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    v_manifest jsonb;
    v_file record;
    v_file_list jsonb;
    v_file_count int;
    v_total_size bigint;
    v_largest_file bigint;
    v_largest_file_path text;
    v_largest_file_size bigint;
    v_chunk_size_bytes bigint;
    v_chunk_index int := 0;
    v_strategy text;
    v_total_chunks int;
    v_current_files jsonb := '[]'::jsonb;
    v_current_size bigint := 0;
    v_chunks_created int := 0;
    v_uncompressed_size numeric;
    v_compression_ratio numeric;
BEGIN
    -- Get file manifest
    SELECT file_manifest INTO v_manifest
    FROM datasets
    WHERE id = p_dataset_id;

    IF v_manifest IS NULL OR jsonb_array_length(v_manifest) = 0 THEN
        RETURN jsonb_build_object(
            'error', 'no file manifest available',
            'strategy', 'fallback_row_range'
        );
    END IF;

    -- Analyze manifest
    SELECT
        COUNT(*)::int,
        MAX((elem->>'size')::bigint),
        SUM((elem->>'size')::bigint),
        (array_agg(elem->>'path' ORDER BY (elem->>'size')::bigint DESC))[1],
        MAX((elem->>'size')::bigint)
    INTO
        v_file_count,
        v_largest_file,
        v_total_size,
        v_largest_file_path,
        v_largest_file_size
    FROM jsonb_array_elements(v_manifest) AS elem;

    v_chunk_size_bytes := p_max_chunk_size_mb * 1048576;

    -- ================================================================
    -- Strategy selection
    -- ================================================================
    -- Rule 1: Single large file > 50MB → byte_range
    IF v_file_count = 1 AND v_largest_file > v_chunk_size_bytes THEN
        v_strategy := 'byte_range';
        v_total_chunks := CEIL(v_largest_file::numeric / v_chunk_size_bytes)::int;

        FOR i IN 0..v_total_chunks - 1 LOOP
            INSERT INTO batch_chunks (
                dataset_id, org_id, chunk_index, status, job_type,
                chunk_strategy, chunk_size_gb,
                byte_range_start, byte_range_end,
                source_file, chunk_vector, payload
            ) VALUES (
                p_dataset_id, p_org_id, i, 'pending', 'preprocess',
                'byte_range',
                LEAST(v_chunk_size_bytes, v_largest_file - i * v_chunk_size_bytes)::numeric / 1073741824.0,
                i * v_chunk_size_bytes,
                LEAST((i + 1) * v_chunk_size_bytes - 1, v_largest_file - 1),
                v_largest_file_path,
                byte_range_vector(i, v_total_chunks, v_largest_file, v_chunk_size_bytes),
                jsonb_build_object('source_file', v_largest_file_path)
            );
            v_chunks_created := v_chunks_created + 1;
        END LOOP;

        RETURN jsonb_build_object(
            'chunks_created', v_chunks_created,
            'strategy', v_strategy,
            'file_count', v_file_count,
            'total_size_bytes', v_total_size
        );
    END IF;

    -- Rule 2: Many small files → file_per_chunk
    IF v_file_count > 1 AND v_largest_file < v_chunk_size_bytes THEN
        v_strategy := 'file_per_chunk';

        FOR v_file IN
            SELECT elem->>'path' AS path,
                   (elem->>'size')::bigint AS size,
                   elem->>'detected_format' AS detected_format,
                   elem->>'md5' AS md5
            FROM jsonb_array_elements(v_manifest) AS elem
            ORDER BY (elem->>'size')::bigint DESC
        LOOP
            v_current_files := v_current_files || jsonb_build_object(
                'path', v_file.path,
                'size', v_file.size,
                'detected_format', v_file.detected_format
            );
            v_current_size := v_current_size + v_file.size;

            IF jsonb_array_length(v_current_files) >= p_max_files_per_chunk
               OR v_current_size >= v_chunk_size_bytes THEN
                INSERT INTO batch_chunks (
                    dataset_id, org_id, chunk_index, status, job_type,
                    chunk_strategy, chunk_size_gb,
                    file_list, chunk_vector, payload
                ) VALUES (
                    p_dataset_id, p_org_id, v_chunk_index, 'pending', 'preprocess',
                    'file_per_chunk',
                    v_current_size::numeric / 1073741824.0,
                    v_current_files,
                    file_list_vector(v_chunk_index, p_max_files_per_chunk, v_current_size),
                    jsonb_build_object('file_count', jsonb_array_length(v_current_files))
                );
                v_chunks_created := v_chunks_created + 1;
                v_chunk_index := v_chunk_index + 1;
                v_current_files := '[]'::jsonb;
                v_current_size := 0;
            END IF;
        END LOOP;

        -- Emit remaining files
        IF jsonb_array_length(v_current_files) > 0 THEN
            INSERT INTO batch_chunks (
                dataset_id, org_id, chunk_index, status, job_type,
                chunk_strategy, chunk_size_gb,
                file_list, chunk_vector, payload
            ) VALUES (
                p_dataset_id, p_org_id, v_chunk_index, 'pending', 'preprocess',
                'file_per_chunk',
                v_current_size::numeric / 1073741824.0,
                v_current_files,
                file_list_vector(v_chunk_index, p_max_files_per_chunk, v_current_size),
                jsonb_build_object('file_count', jsonb_array_length(v_current_files))
            );
            v_chunks_created := v_chunks_created + 1;
        END IF;

        RETURN jsonb_build_object(
            'chunks_created', v_chunks_created,
            'strategy', v_strategy,
            'file_count', v_file_count,
            'total_size_bytes', v_total_size
        );
    END IF;

    -- Rule 3: Structured data or mixed → existing row_range
    PERFORM pre_chunk_dataset_smart(p_dataset_id, p_org_id);
    RETURN jsonb_build_object(
        'strategy', 'row_range',
        'delegated_to', 'pre_chunk_dataset_smart'
    );
END;
$$;
```

### 2.3 Vector Encoding Helpers for New Strategies

```sql
-- 16-dim vector for byte-range chunks
CREATE OR REPLACE FUNCTION public.byte_range_vector(
    chunk_index int, total_chunks int,
    file_size bigint, chunk_size_bytes bigint
) RETURNS public.vector
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    vec double precision[] := array_fill(0::double precision, ARRAY[16]);
    norm double precision;
    chunk_gb double precision := LEAST(chunk_size_bytes, file_size - chunk_index * chunk_size_bytes)::numeric / 1073741824.0;
BEGIN
    vec[1] := chunk_index::double precision / GREATEST(total_chunks, 1)::double precision;       -- position
    vec[2] := LEAST(chunk_gb / 8.0, 1.0);                                                       -- relative size
    vec[3] := LEAST(total_chunks::double precision / 100.0, 1.0);                                -- total density
    vec[4] := CASE WHEN chunk_index = total_chunks - 1 THEN 1.0 ELSE 0.0 END;                    -- tail marker
    vec[5] := 1.0;  -- strategy = byte_range (dim 4=1.0, dim 5=0.0)
    vec[6] := 0.0;
    -- Dims 7-16: reserved for device affinity (0.5 neutral by default)
    FOR i IN 7..16 LOOP
        vec[i] := 0.5;
    END LOOP;
    -- L2 normalize
    norm := sqrt((SELECT SUM(v * v) FROM unnest(vec) AS v));
    IF norm > 0 THEN
        RETURN (SELECT array_agg(v / norm) FROM unnest(vec) AS v)::public.vector;
    END IF;
    RETURN vec::public.vector;
END;
$$;

-- 16-dim vector for file-per-chunk chunks
CREATE OR REPLACE FUNCTION public.file_list_vector(
    chunk_index int, max_files int, total_size_bytes bigint
) RETURNS public.vector
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    vec double precision[] := array_fill(0::double precision, ARRAY[16]);
    norm double precision;
BEGIN
    vec[1] := 0.0;    -- no meaningful position
    vec[2] := LEAST(total_size_bytes::numeric / (8589934592.0), 1.0);  -- relative size (8GB max)
    vec[3] := LEAST(max_files::double precision / 100.0, 1.0);
    vec[4] := 1.0;    -- always tail (each file-per-chunk is self-contained)
    vec[5] := 0.0;    -- strategy = file_per_chunk (dim 4=0.0, dim 5=1.0)
    vec[6] := 1.0;
    FOR i IN 7..16 LOOP
        vec[i] := 0.5;
    END LOOP;
    norm := sqrt((SELECT SUM(v * v) FROM unnest(vec) AS v));
    IF norm > 0 THEN
        RETURN (SELECT array_agg(v / norm) FROM unnest(vec) AS v)::public.vector;
    END IF;
    RETURN vec::public.vector;
END;
$$;
```

### 2.4 Update `pre_chunk_dataset` Edge Function

**File:** `supabase/functions/pre_chunk_dataset/index.ts`

```typescript
// After existing supabase.rpc("pre_chunk_dataset_smart", ...) call:
// Check if dataset has non-tabular files → use pre_chunk_filebased instead

const { data: dataset } = await supabase
  .from("datasets")
  .select("file_manifest, total_size_gb")
  .eq("id", dataset_id)
  .single();

const manifest = dataset?.file_manifest || [];
const isNonTabular = manifest.some((f: any) =>
  !f.detected_format?.match(/^(csv|json|parquet|jsonl)$/)
);

if (isNonTabular && manifest.length > 0) {
  const { data: chunkResult, error } = await supabase.rpc("pre_chunk_filebased", {
    p_dataset_id: dataset_id,
    p_org_id: authResult.orgId,
    p_max_chunk_size_mb: 50,
    p_max_files_per_chunk: 50,
  });
  if (error) throw error;
  return new Response(JSON.stringify(chunkResult), { ... });
}
```

### 2.5 Update `plan_dataset_chunks` Edge Function

**File:** `supabase/functions/plan_dataset_chunks/index.ts`

```typescript
// Add chunk_strategy to step payload
const step = steps[step_index];
const chunkStrategy = step.chunk_strategy || "row_range";

// When selecting pending chunks, filter by strategy
let query = supabase
  .from("batch_chunks")
  .select("*")
  .eq("dataset_id", dataset_id)
  .eq("status", "pending")
  .eq("job_type", "process");

if (chunkStrategy !== "row_range") {
  query = query.eq("chunk_strategy", chunkStrategy);
}

// Insert chunk with strategy info
const insertPayload = {
  ...existingPayload,
  chunk_strategy: chunkStrategy,
  byte_range_start: null,  // populated if strategy = byte_range
  byte_range_end: null,    // populated if strategy = byte_range
  source_file: null,       // populated if strategy = byte_range
};
```

### 2.6 Agent: Update `ProcessPayload`

**File:** `internal/dispatcher/handlers_unix.go` (line ~598)

```go
type ProcessPayload struct {
    // Existing fields...
    ChunkStrategy  string   `json:"chunk_strategy"`
    ByteRangeStart int64    `json:"byte_range_start,omitempty"`
    ByteRangeEnd   int64    `json:"byte_range_end,omitempty"`
    FileList       []string `json:"file_list,omitempty"`
    SourceFilePath string   `json:"source_file_path,omitempty"`
}
```

### 2.7 Agent: Update `executeProcessChunk`

```go
func executeProcessChunk(ctx context.Context, payload ProcessPayload, workDir string) error {
    switch payload.ChunkStrategy {
    case "", "row_range":
        return executeProcessChunkRowRange(ctx, payload, workDir) // existing

    case "byte_range":
        return executeProcessChunkByteRange(ctx, payload, workDir)

    case "file_per_chunk":
        return executeProcessChunkFileList(ctx, payload, workDir)

    default:
        return fmt.Errorf("unknown chunk_strategy: %s", payload.ChunkStrategy)
    }
}

func executeProcessChunkByteRange(ctx context.Context, payload ProcessPayload, workDir string) error {
    // 1. Open source file
    srcFile, err := os.Open(payload.SourceFilePath)
    if err != nil {
        return fmt.Errorf("open source for byte range: %w", err)
    }
    defer srcFile.Close()

    // 2. Seek to start
    _, err = srcFile.Seek(payload.ByteRangeStart, io.SeekStart)
    if err != nil {
        return fmt.Errorf("seek to byte range start %d: %w", payload.ByteRangeStart, err)
    }

    // 3. Read range into chunk input file
    rangeSize := payload.ByteRangeEnd - payload.ByteRangeStart + 1
    chunkInputPath := filepath.Join(workDir, "chunk_input.bin")
    chunkFile, err := os.Create(chunkInputPath)
    if err != nil {
        return fmt.Errorf("create chunk input: %w", err)
    }
    defer chunkFile.Close()

    _, err = io.CopyN(chunkFile, srcFile, rangeSize)
    if err != nil {
        return fmt.Errorf("copy byte range: %w", err)
    }

    // 4. Construct plugin input payload with byte range info
    input := map[string]any{
        "job_id":         payload.JobID,
        "chunk_id":       payload.ChunkID,
        "chunk_index":    payload.ChunkIndex,
        "input_path":     chunkInputPath,
        "byte_range_start": payload.ByteRangeStart,
        "byte_range_end":   payload.ByteRangeEnd,
        "config":         payload.Config,
    }

    // 5. Run plugin (same as existing, stdin gets input JSON)
    return runPlugin(ctx, payload.PluginPath, input, workDir)
}

func executeProcessChunkFileList(ctx context.Context, payload ProcessPayload, workDir string) error {
    // 1. Symlink all files in the file list into work dir
    for _, filePath := range payload.FileList {
        dest := filepath.Join(workDir, filepath.Base(filePath))
        if err := os.Symlink(filePath, dest); err != nil {
            return fmt.Errorf("symlink %s: %w", filePath, err)
        }
    }

    // 2. Pass file list to plugin via stdin
    input := map[string]any{
        "job_id":     payload.JobID,
        "chunk_id":   payload.ChunkID,
        "chunk_index": payload.ChunkIndex,
        "work_dir":   workDir,
        "files":      payload.FileList,
        "config":     payload.Config,
    }

    return runPlugin(ctx, payload.PluginPath, input, workDir)
}
```

### 2.8 Agent: Update `executeMergeDataset`

```go
func executeMergeDataset(ctx context.Context, payload MergePayload) error {
    // Determine merge strategy from first chunk
    chunkStrategy := inferChunkStrategy(payload.ChunkIDs)

    switch chunkStrategy {
    case "row_range":
        return mergeRowRange(ctx, payload) // existing

    case "byte_range":
        return mergeByteRange(ctx, payload) // concatenate in order

    case "file_per_chunk":
        return mergeFileList(ctx, payload) // copy to output dir

    default:
        // Fallback: try existing merge
        return mergeRowRange(ctx, payload)
    }
}

func mergeByteRange(ctx context.Context, payload MergePayload) error {
    // Sort chunks by chunk_index
    // Concatenate output files in order
    // For video: write file list for ffmpeg concat (future)
    // For raw binary: simple append
    outPath := payload.OutputPath
    outFile, err := os.Create(outPath)
    if err != nil {
        return err
    }
    defer outFile.Close()

    for _, chunk := range payload.Chunks {
        data, err := os.ReadFile(chunk.OutputPath)
        if err != nil {
            return fmt.Errorf("read chunk %s: %w", chunk.ChunkID, err)
        }
        if _, err := outFile.Write(data); err != nil {
            return err
        }
    }
    return nil
}
```

### 2.9 Update `claim_jobs_for_device` RPC

```sql
-- In the claim_jobs_for_device function, add:
-- If claiming a byte_range job, ensure device has sufficient disk and bandwidth
-- If claiming a file_per_chunk job, prefer devices with fast filesystem

-- Add to the WHERE clause:
AND (
    (bc.chunk_strategy = 'byte_range' AND d.available_disk_gb > 10)  -- need temp space
    OR bc.chunk_strategy != 'byte_range'
)
```

---

## Phase 3: GPU-Aware Scheduling

**Goal:** Plugin manifest declares `requires_gpu: true` → scheduler matches GPU-capable agents → sandbox allows GPU device access.

### 3.1 Agent: `internal/plugin/manifest.go`

```go
type PluginResources struct {
    MemoryMB       int64   `json:"memory_mb"`
    CPUSeconds     int64   `json:"cpu_seconds"`
    CPULimit       float64 `json:"cpu_limit,omitempty"`
    TimeoutSeconds int64   `json:"timeout_seconds"`
    RequiresGPU    bool    `json:"requires_gpu,omitempty"`    // NEW
    GPUMemoryMB    int64   `json:"gpu_memory_mb,omitempty"`  // NEW
}
```

### 3.2 Agent: `internal/sandbox/sandboxer.go`

```go
type PluginResources struct {
    MemoryMB       int64   `json:"memory_mb"`
    CPUSeconds     int64   `json:"cpu_seconds"`
    CPULimit       float64 `json:"cpu_limit,omitempty"`
    TimeoutSeconds int64   `json:"timeout_seconds"`
    RequiresGPU    bool    `json:"requires_gpu,omitempty"`    // NEW
    GPUMemoryMB    int64   `json:"gpu_memory_mb,omitempty"`  // NEW
}
```

### 3.3 Agent: `internal/plugin/sandbox.go`

```go
// In RunSandboxedPlugin, add to field copy:
pm := sandbox.PluginManifest{
    Resources: sandbox.PluginResources{
        // ...existing...
        RequiresGPU:    manifest.Resources.RequiresGPU,
        GPUMemoryMB:    manifest.Resources.GPUMemoryMB,
    },
}

// Validate GPU availability before execution:
if manifest.Resources.RequiresGPU {
    specs := sysinfo.Detect()
    if specs.GPUModel == "" {
        return "", fmt.Errorf("plugin %s requires GPU but device has none", manifest.Name)
    }
    if manifest.Resources.GPUMemoryMB > 0 &&
       specs.GPUMemoryFreeGB * 1024 < float64(manifest.Resources.GPUMemoryMB) {
        return "", fmt.Errorf("plugin %s requires %d MB GPU memory but only %.0f MB free",
            manifest.Name, manifest.Resources.GPUMemoryMB, specs.GPUMemoryFreeGB * 1024)
    }
}
```

### 3.4 Agent: `internal/sandbox/sandboxer_linux.go`

```go
// In Execute(), around the cgroup setup:
if env.Manifest.Resources.RequiresGPU {
    // Allow NVIDIA devices in cgroup
    nvidiaAllow := []byte("c 195:* rwm\n") // NVIDIA /dev/nvidia*
    os.WriteFile(cgPath+"/device.allow", nvidiaAllow, 0644)

    // Set NVIDIA_VISIBLE_DEVICES environment variable
    env.Cmd.Env = append(env.Cmd.Env,
        "NVIDIA_VISIBLE_DEVICES=all",
        "NVIDIA_DRIVER_CAPABILITIES=compute,utility",
    )

    // Bind mount /dev/dri for GPU access
    // (already handled by namespace setup; just ensure device nodes visible)
}
```

### 3.5 Agent: `internal/sandbox/sandboxer_darwin.go`

```go
// In the Seatbelt profile template, add:
const macSandboxProfileTpl = `...
(allow device* (subpath "/dev"))
...`

// Or more selectively:
// (allow iokit-open (require-all
//     (iokit-registry-entry-class "IOAccelerator")
//     (iokit-property "MetalPluginName" "com.apple.Metal")))
```

### 3.6 Agent: `internal/backend/execution_client.go`

```go
// In DeviceHeartbeat struct, add:
type DeviceHeartbeat struct {
    // ...existing...
    GPUAvailable   bool    `json:"gpu_available"`
    GPUModel       string  `json:"gpu_model"`
    GPUMemoryFreeGB float64 `json:"gpu_memory_free_gb"`   // NEW
    GPUMemoryTotalGB float64 `json:"gpu_memory_total_gb"` // NEW
}

// Populate in sendHeartbeat():
heartbeat.GPUMemoryFreeGB = sysinfo.Detect().GPUMemoryFreeGB
heartbeat.GPUMemoryTotalGB = sysinfo.Detect().GPUMemoryTotalGB
```

### 3.7 Supabase: Update `pre_chunk_dataset_smart` (add GPU to scoring)

```sql
-- In the device weight calculation (line ~4576-4582 of remote_schema.sql):
-- Add GPU bonus factor when chunk requires GPU:

IF v_chunk_requires_gpu THEN
    weight := weight *
        CASE WHEN d.gpu_available THEN
            1.0 + d.gpu_capability_score  -- bonus for GPU devices
        ELSE
            0.0  -- exclude non-GPU devices entirely
        END;
END IF;

-- Add to resource_weight calculation:
resource_weight := resource_weight *
    CASE WHEN v_chunk_requires_gpu AND d.gpu_available THEN 1.5
         WHEN v_chunk_requires_gpu THEN 0.0
         ELSE 1.0
    END;
```

### 3.8 Supabase: Update `claim_jobs_for_device` RPC

```sql
-- In the claim query, add GPU filtering to the WHERE clause:
-- If the job/execution requires GPU, only assign to GPU-capable devices:

AND (
    NOT EXISTS (
        SELECT 1 FROM batch_chunks bc2
        WHERE bc2.id = j.batch_chunk_id
        AND (bc2.payload->>'requires_gpu')::bool = true
    )
    OR (
        EXISTS (
            SELECT 1 FROM batch_chunks bc2
            WHERE bc2.id = j.batch_chunk_id
            AND (bc2.payload->>'requires_gpu')::bool = true
        )
        AND d.gpu_available = true
    )
)
```

### 3.9 Supabase: Update `match_best_device` RPC

```sql
-- Add GPU to the score formula:
score = (1 - cosine_distance(device_profile_vector, chunk_vector))
      * (0.5 + (cpu_cores_free / total_cpu_cores) * 0.3
             + (memory_free_gb / total_memory_gb) * 0.2
             + CASE WHEN chunk_requires_gpu AND d.gpu_available THEN 0.3
                    ELSE 0 END)                               -- GPU bonus
      * MAX(0.1, (max_concurrency - active_jobs) / max_concurrency)
      * CASE WHEN chunk_requires_gpu AND NOT d.gpu_available THEN 0.0
             ELSE 1.0 END                                     -- GPU gating
```

---

## Execution Order

```
Phase 1 (Rich Media Scan)  →  Phase 3 (GPU Scheduling)  →  Phase 2 (Byte-Range Chunking)
    2-3 files, no schema          6 agent files + 3 SQL       6 agent files + 4 SQL + 2 edge fn
    1-2 days                      1-2 days                    3-5 days
```

### Why this order

1. **Phase 1 first** — Scan is the entry point. Fixing it unblocks everything else. No DB changes needed — just adding new extractor functions and wiring them in. The `file_manifest` JSONB output from Phase 1 is consumed by Phase 2's chunking algorithm.

2. **Phase 3 next** — Small, self-contained. Manifest changes, sandbox enforcement, and RPC adjustments. Phase 2 (chunking) needs the chunk_strategy to eventually carry GPU requirement flags, but Phase 3 can work with the existing row_range model first.

3. **Phase 2 last** — Most complex. Touches database schema, edge functions, agent execution paths, and merge logic. Depends on Phase 1's `file_manifest` output. Should be done last when the data model is stable.

---

## Testing Strategy

| Phase | Test Type | What |
|-------|-----------|------|
| 1 | Unit (Go) | For each new extractor: place test file in `testdata/`, call extractor, verify expected metadata fields |
| 1 | Integration | Upload an image → scan → verify `image_width`, `image_height` in scan output |
| 3 | Unit (Go) | Plugin with `requires_gpu: true` → verify rejection on non-GPU agent |
| 3 | Unit (Go) | Plugin with `requires_gpu: true` + GPU available → verify `/dev/nvidia*` allowed in cgroup |
| 3 | Integration | Register GPU plugin → claim job on GPU device → verify job executes on that device |
| 2 | Unit (SQL) | Call `pre_chunk_filebased` with mock manifest → verify correct chunk counts, strategies, byte ranges |
| 2 | Integration | Upload 200MB video → scan → chunk → verify byte-range chunks created in order |
| 2 | Integration | Upload 100 small images → scan → chunk → verify file-per-chunk groups created |
| 2 | Integration | Process byte-range chunk → merge → verify output identical to original file |
