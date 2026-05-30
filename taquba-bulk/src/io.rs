//! Input and output adapters for bulk runs.
//!
//! Line-delimited JSON (JSONL): one input item per line in, one result
//! record per line out. [`read_jsonl`] decodes a reader into typed
//! items; [`OutputSink`] is the write side, with [`JsonlSink`] as the
//! built-in implementation. Other sources (CSV, S3 prefixes) and sinks can
//! be added by implementing the traits without touching the runner.

use std::io::{BufRead, Write};
use std::sync::Mutex;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// Decode a JSONL reader into an iterator of typed items. Each non-empty
/// line is parsed as one `T`; blank lines are skipped. Decode errors are
/// yielded as `Err` so the caller decides whether to stop or continue.
pub fn read_jsonl<T, R>(reader: R) -> impl Iterator<Item = Result<T>>
where
    T: DeserializeOwned,
    R: BufRead,
{
    reader.lines().filter_map(|line| match line {
        Ok(line) if line.trim().is_empty() => None,
        Ok(line) => Some(serde_json::from_str::<T>(&line).map_err(Error::from)),
        Err(err) => Some(Err(Error::from(err))),
    })
}

/// One result record handed to an [`OutputSink`] as an item completes.
#[derive(Debug)]
pub struct OutputRecord<'a> {
    /// The completed item's run identifier.
    pub run_id: &'a str,
    /// Terminal status, as the canonical lowercase string
    /// (`"succeeded"`, `"failed"`, `"cancelled"`).
    pub status: &'a str,
    /// The pipeline output, present only for succeeded items.
    pub output: Option<Value>,
    /// A failure reason, present for failed or cancelled items when one was
    /// recorded.
    pub error: Option<&'a str>,
}

/// The write side of a bulk run. Implementations receive one
/// [`OutputRecord`] per item as it reaches a terminal state, possibly from
/// many worker tasks concurrently, so `write` takes `&self` and must handle
/// its own synchronization.
pub trait OutputSink: Send + Sync {
    /// Persist one completed item's record.
    fn write(&self, record: &OutputRecord<'_>) -> Result<()>;

    /// Flush any buffered output. Called once when the run finishes. The
    /// default does nothing.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// An [`OutputSink`] that writes one JSON object per line to an underlying
/// writer. Each line carries `run_id`, `status`, and either `output` (for
/// succeeded items) or `error` (when one is present). Writes are serialized
/// through a mutex so the sink can be shared across worker tasks.
pub struct JsonlSink<W: Write> {
    writer: Mutex<W>,
}

impl<W: Write> JsonlSink<W> {
    /// Wrap a writer as a JSONL sink. Pass a buffered writer (e.g.
    /// [`std::io::BufWriter`]) for file or socket targets.
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl<W: Write + Send> OutputSink for JsonlSink<W> {
    fn write(&self, record: &OutputRecord<'_>) -> Result<()> {
        let mut obj = Map::new();
        obj.insert("run_id".into(), Value::String(record.run_id.to_string()));
        obj.insert("status".into(), Value::String(record.status.to_string()));
        if let Some(output) = &record.output {
            obj.insert("output".into(), output.clone());
        }
        if let Some(error) = record.error {
            obj.insert("error".into(), Value::String(error.to_string()));
        }
        let line = serde_json::to_string(&Value::Object(obj))?;
        let mut writer = self.writer.lock().unwrap();
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.writer.lock().unwrap().flush()?;
        Ok(())
    }
}

/// An [`OutputSink`] that discards every record. The default sink, for runs
/// whose pipeline produces its results as side effects (writing to a
/// database, calling an API) rather than through the output stream.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

impl OutputSink for NullSink {
    fn write(&self, _record: &OutputRecord<'_>) -> Result<()> {
        Ok(())
    }
}

/// Convert a serializable pipeline output into a JSON value for an
/// [`OutputRecord`].
pub(crate) fn output_to_value<O: Serialize>(output: &O) -> Result<Value> {
    Ok(serde_json::to_value(output)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        id: u32,
        name: String,
    }

    #[test]
    fn read_jsonl_decodes_lines_and_skips_blanks() {
        let input = "{\"id\":1,\"name\":\"a\"}\n\n{\"id\":2,\"name\":\"b\"}\n";
        let items: Vec<Item> = read_jsonl(input.as_bytes())
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            items,
            vec![
                Item {
                    id: 1,
                    name: "a".into()
                },
                Item {
                    id: 2,
                    name: "b".into()
                },
            ],
        );
    }

    #[test]
    fn read_jsonl_yields_error_for_bad_line() {
        let input = "{\"id\":1,\"name\":\"a\"}\nnot json\n";
        let results: Vec<Result<Item>> = read_jsonl(input.as_bytes()).collect();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
    }

    #[test]
    fn jsonl_sink_writes_one_object_per_line() {
        let buf: Vec<u8> = Vec::new();
        let sink = JsonlSink::new(buf);
        sink.write(&OutputRecord {
            run_id: "item-0",
            status: "succeeded",
            output: Some(serde_json::json!({"n": 42})),
            error: None,
        })
        .unwrap();
        sink.write(&OutputRecord {
            run_id: "item-1",
            status: "failed",
            output: None,
            error: Some("boom"),
        })
        .unwrap();
        sink.flush().unwrap();

        // Recover the buffer by writing into a fresh sink and reading lines.
        let bytes = sink.writer.into_inner().unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["run_id"], "item-0");
        assert_eq!(first["status"], "succeeded");
        assert_eq!(first["output"]["n"], 42);
        assert!(first.get("error").is_none());

        let second: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["status"], "failed");
        assert_eq!(second["error"], "boom");
        assert!(second.get("output").is_none());
    }
}
