// Package dogrus provides a hook for the logrus logging package.
// This hook allow to easily send every log entry to Datadog without the need
// of an external agent.
package dogrus

import (
	"bytes"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// Hook is a logrus hook that sends logs to Datadog using HTTP and
// logrus JSONFormatter for marshalling entries.
// Logs are sent in batches to avoid creation of too many connections.
// Use New() to create a initialize a new hook.
type Hook struct {
	key       string
	opts      Opts
	lastFlush time.Time
	timer     *time.Timer
	batch     chan []byte
}

// Opts are variables for tuning perfomances.
// All options can be left empty and they will be filled with default values.
type Opts struct {
	// FlushPeriod sets the interval of time to wait before triggering a flush.
	FlushPeriod time.Duration

	// MaxBatchSize sets the size of the batch that will force a flush.
	// A MaxBatchSize of 1 will make each entry sent instantly.
	MaxBatchSize int

	// PostURL is the address where HTTP request will be sent.
	// By default is Datadog EU server (https://http-intake.logs.datadoghq.eu/v1/input).
	PostURL string

	// Formatter is the formatter used by this hook to marshal each logrus
	// entry into a JSON.
	// It defaults to logrus.JSONFormatter configured with standard Datadog
	// keys.
	Formatter logrus.Formatter
}

// New creates a new Hook using the API key provided.
// Optionally, opts can be provided for some performance tuning.
func New(apiKey string, opts Opts) *Hook {
	if opts.FlushPeriod == 0 {
		opts.FlushPeriod = 30 * time.Second
	}

	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = 30
	}

	if opts.PostURL == "" {
		opts.PostURL = "https://http-intake.logs.datadoghq.eu/v1/input"
	}

	if opts.Formatter == nil {
		opts.Formatter = &logrus.JSONFormatter{
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
		}
	}

	d := &Hook{
		key:   apiKey,
		opts:  opts,
		batch: make(chan []byte, opts.MaxBatchSize),
	}
	d.timer = time.AfterFunc(opts.FlushPeriod, func() {
		d.Flush()
	})

	return d
}

// Fire is automatically called by logrus everytime a log entry is created.
func (d *Hook) Fire(entry *logrus.Entry) error {
	// format entry into json []byte
	result, err := d.opts.Formatter.Format(entry)
	if err != nil {
		return err
	}

	// add entry to batch
	d.batch <- result

	// if batch is big enough, flush it
	if len(d.batch) == cap(d.batch) {
		d.Flush()
	}

	return err
}

// Levels is called by logrus to check what levels are handler by this hook.
func (d *Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Flush flushes the current batch of log entries, sending them to Datadog
// server.
func (d *Hook) Flush() error {
	currentBatch := d.batch
	d.batch = make(chan []byte, d.opts.MaxBatchSize)

	close(currentBatch)

	d.lastFlush = time.Now()

	// prepare json body
	buffer := new(bytes.Buffer)

	_, err := buffer.WriteString("[")
	if err != nil {
		return err
	}

	for log := range currentBatch {
		_, err := buffer.Write(log)
		if err != nil {
			return err
		}

		// if there are still elements, a trailing comma is needed to separate
		// them
		if len(currentBatch) > 0 {
			_, err = buffer.WriteRune(',')
			if err != nil {
				return err
			}
		}
	}

	buffer.WriteString("]")

	// prepare http request
	req, err := http.NewRequest("POST", d.opts.PostURL, buffer)
	if err != nil {
		return err
	}

	req.Header.Set("DD-API-KEY", d.key)
	req.Header.Set("Content-Type", "application/json")

	// do request
	client := &http.Client{}

	_, err = client.Do(req)
	if err != nil {
		return err
	}

	d.scheduleFlush()

	return nil
}

func (d *Hook) scheduleFlush() {
	d.timer.Reset(d.opts.FlushPeriod)
}
