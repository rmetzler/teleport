/*
Copyright 2020 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package filesessions

import (
	"bytes"
	"context"
	"go.uber.org/atomic"
	"testing"
	"time"

	"github.com/gravitational/teleport/lib/events"
	"github.com/gravitational/teleport/lib/fixtures"
	"github.com/gravitational/teleport/lib/session"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"

	"gopkg.in/check.v1"
)

type UploaderSuite struct {
}

var _ = check.Suite(&UploaderSuite{})

func (s *UploaderSuite) SetUpSuite(c *check.C) {
	utils.InitLoggerForTests(testing.Verbose())
}

// TestUploadOK verifies successfull upload run
func (s *UploaderSuite) TestUploadOK(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clock := clockwork.NewFakeClock()

	eventsC := make(chan events.UploadEvent, 100)
	memUploader := events.NewMemoryUploader(eventsC)
	streamer, err := events.NewProtoStreamer(events.ProtoStreamerConfig{
		Uploader: memUploader,
	})
	c.Assert(err, check.IsNil)

	scanDir := c.MkDir()
	scanPeriod := 10 * time.Second
	uploader, err := NewUploader(UploaderConfig{
		Context:    ctx,
		ScanDir:    scanDir,
		ScanPeriod: scanPeriod,
		Streamer:   streamer,
		Clock:      clock,
	})
	c.Assert(err, check.IsNil)
	go uploader.Serve()
	// wait until uploader blocks on the clock

	clock.BlockUntil(1)

	defer uploader.Close()

	fileStreamer, err := NewStreamer(scanDir)
	c.Assert(err, check.IsNil)

	inEvents := events.GenerateSession(1024)
	sid := inEvents[0].(events.SessionMetadataGetter).GetSessionID()

	emitStream(ctx, c, fileStreamer, inEvents)

	// initiate the scan by advancing clock past
	// block period
	clock.Advance(scanPeriod + time.Second)

	var event events.UploadEvent
	select {
	case event = <-eventsC:
		c.Assert(event.SessionID, check.Equals, sid)
		c.Assert(event.Error, check.IsNil)
	case <-ctx.Done():
		c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
	}

	// read the upload and make sure the data is equal
	outEvents := readStream(ctx, c, event.UploadID, memUploader)

	fixtures.DeepCompareSlices(c, inEvents, outEvents)
}

// TestUploadParallel verifies several parallel uploads that have to wait
// for semaphore that limits paralell
func (s *UploaderSuite) TestUploadParallel(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clock := clockwork.NewFakeClock()

	eventsC := make(chan events.UploadEvent, 100)
	memUploader := events.NewMemoryUploader(eventsC)
	streamer, err := events.NewProtoStreamer(events.ProtoStreamerConfig{
		Uploader: memUploader,
	})
	c.Assert(err, check.IsNil)

	scanDir := c.MkDir()
	scanPeriod := 10 * time.Second
	uploader, err := NewUploader(UploaderConfig{
		Context:           ctx,
		ScanDir:           scanDir,
		ScanPeriod:        scanPeriod,
		Streamer:          streamer,
		Clock:             clock,
		ConcurrentUploads: 2,
	})
	c.Assert(err, check.IsNil)
	go uploader.Serve()
	// wait until uploader blocks on the clock
	clock.BlockUntil(1)

	defer uploader.Close()

	sessions := make(map[string][]events.AuditEvent)

	for i := 0; i < 5; i++ {
		fileStreamer, err := NewStreamer(scanDir)
		c.Assert(err, check.IsNil)

		sessionEvents := events.GenerateSession(1024)
		sid := sessionEvents[0].(events.SessionMetadataGetter).GetSessionID()

		emitStream(ctx, c, fileStreamer, sessionEvents)
		sessions[sid] = sessionEvents
	}

	// initiate the scan by advancing the clock past
	// block period
	clock.Advance(scanPeriod + time.Second)

	for range sessions {
		var event events.UploadEvent
		var sessionEvents []events.AuditEvent
		var found bool
		select {
		case event = <-eventsC:
			log.Debugf("Got upload event %v", event)
			c.Assert(event.Error, check.IsNil)
			sessionEvents, found = sessions[event.SessionID]
			c.Assert(found, check.Equals, true,
				check.Commentf("session %q is not expected, possible duplicate event", event.SessionID))
		case <-ctx.Done():
			c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
		}

		// read the upload and make sure the data is equal
		outEvents := readStream(ctx, c, event.UploadID, memUploader)

		fixtures.DeepCompareSlices(c, sessionEvents, outEvents)

		delete(sessions, event.SessionID)
	}
}

type resumeTestCase struct {
	name    string
	newTest func(streamer events.Streamer) resumeTestTuple
	// retries is how many times the uploader will retry the upload
	// after the first upload attempt fails
	retries int
}

type resumeTestTuple struct {
	streamer *events.CallbackStreamer
	verify   func(c *check.C, tc resumeTestCase)
}

// TestUploadResume verifies successfull upload run after the stream has been interrupted
func (s *UploaderSuite) TestUploadResume(c *check.C) {
	testCases := []resumeTestCase{
		{
			name:    "stream terminates in the middle of submission",
			retries: 1,
			newTest: func(streamer events.Streamer) resumeTestTuple {
				streamResumed := atomic.NewUint64(0)
				terminateConnection := atomic.NewUint64(1)

				callbackStreamer, err := events.NewCallbackStreamer(events.CallbackStreamerConfig{
					Inner: streamer,
					OnEmitAuditEvent: func(ctx context.Context, sid session.ID, event events.AuditEvent) error {
						if event.GetIndex() > 600 && terminateConnection.CAS(1, 0) == true {
							log.Debugf("Terminating connection at event %v", event.GetIndex())
							return trace.ConnectionProblem(nil, "connection terminated")
						}
						return nil
					},
					OnResumeAuditStream: func(ctx context.Context, sid session.ID, uploadID string, streamer events.Streamer) (events.Stream, error) {
						stream, err := streamer.ResumeAuditStream(ctx, sid, uploadID)
						c.Assert(err, check.IsNil)
						streamResumed.Inc()
						return stream, nil
					},
				})
				c.Assert(err, check.IsNil)
				return resumeTestTuple{
					streamer: callbackStreamer,
					verify: func(c *check.C, tc resumeTestCase) {
						c.Assert(int(streamResumed.Load()), check.Equals, 1, check.Commentf(tc.name))
					},
				}
			},
		},
		{
			name:    "stream terminates multiple times at different points of the submission",
			retries: 10,
			newTest: func(streamer events.Streamer) resumeTestTuple {
				streamResumed := atomic.NewUint64(0)
				terminateConnection := atomic.NewUint64(0)

				callbackStreamer, err := events.NewCallbackStreamer(events.CallbackStreamerConfig{
					Inner: streamer,
					OnEmitAuditEvent: func(ctx context.Context, sid session.ID, event events.AuditEvent) error {
						if event.GetIndex() > 600 && terminateConnection.Inc() <= 10 {
							log.Debugf("Terminating connection #%v at event %v", terminateConnection.Load(), event.GetIndex())
							return trace.ConnectionProblem(nil, "connection terminated")
						}
						return nil
					},
					OnResumeAuditStream: func(ctx context.Context, sid session.ID, uploadID string, streamer events.Streamer) (events.Stream, error) {
						stream, err := streamer.ResumeAuditStream(ctx, sid, uploadID)
						c.Assert(err, check.IsNil)
						streamResumed.Inc()
						return stream, nil
					},
				})
				c.Assert(err, check.IsNil)
				return resumeTestTuple{
					streamer: callbackStreamer,
					verify: func(c *check.C, tc resumeTestCase) {
						c.Assert(int(streamResumed.Load()), check.Equals, 10, check.Commentf(tc.name))
					},
				}
			},
		},
		{
			name:    "stream recreates submission if upload is not found",
			retries: 1,
			newTest: func(streamer events.Streamer) resumeTestTuple {
				streamCreated := atomic.NewUint64(0)
				terminateConnection := atomic.NewUint64(1)

				callbackStreamer, err := events.NewCallbackStreamer(events.CallbackStreamerConfig{
					Inner: streamer,
					OnEmitAuditEvent: func(ctx context.Context, sid session.ID, event events.AuditEvent) error {
						if event.GetIndex() > 600 && terminateConnection.CAS(1, 0) == true {
							log.Debugf("Terminating connection at event %v", event.GetIndex())
							return trace.ConnectionProblem(nil, "connection terminated")
						}
						return nil
					},
					OnCreateAuditStream: func(ctx context.Context, sid session.ID, streamer events.Streamer) (events.Stream, error) {
						stream, err := streamer.CreateAuditStream(ctx, sid)
						c.Assert(err, check.IsNil)
						streamCreated.Inc()
						return stream, nil
					},
					OnResumeAuditStream: func(ctx context.Context, sid session.ID, uploadID string, streamer events.Streamer) (events.Stream, error) {
						return nil, trace.NotFound("stream not found")
					},
				})
				c.Assert(err, check.IsNil)
				return resumeTestTuple{
					streamer: callbackStreamer,
					verify: func(c *check.C, tc resumeTestCase) {
						c.Assert(int(streamCreated.Load()), check.Equals, 2, check.Commentf(tc.name))
					},
				}
			},
		},
	}
	for _, tc := range testCases {
		s.runResume(c, tc)
	}
}

// runResume runs resume scenario based on the test case specification
func (s *UploaderSuite) runResume(c *check.C, testCase resumeTestCase) {
	log.Debugf("Running test %q.", testCase.name)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clock := clockwork.NewFakeClock()
	eventsC := make(chan events.UploadEvent, 100)
	memUploader := events.NewMemoryUploader(eventsC)
	streamer, err := events.NewProtoStreamer(events.ProtoStreamerConfig{
		Uploader:       memUploader,
		MinUploadBytes: 1024,
	})
	c.Assert(err, check.IsNil)

	test := testCase.newTest(streamer)

	scanDir := c.MkDir()
	scanPeriod := 10 * time.Second
	uploader, err := NewUploader(UploaderConfig{
		EventsC:    eventsC,
		Context:    ctx,
		ScanDir:    scanDir,
		ScanPeriod: scanPeriod,
		Streamer:   test.streamer,
		Clock:      clock,
	})
	c.Assert(err, check.IsNil)
	go uploader.Serve()
	// wait until uploader blocks on the clock
	clock.BlockUntil(1)

	defer uploader.Close()

	fileStreamer, err := NewStreamer(scanDir)
	c.Assert(err, check.IsNil)

	inEvents := events.GenerateSession(1024)
	sid := inEvents[0].(events.SessionMetadataGetter).GetSessionID()

	emitStream(ctx, c, fileStreamer, inEvents)

	// initiate the scan by advancing clock past
	// block period
	clock.Advance(scanPeriod + time.Second)

	// wait for the upload failure
	var event events.UploadEvent
	select {
	case event = <-eventsC:
		c.Assert(event.SessionID, check.Equals, sid)
		c.Assert(event.Error, check.FitsTypeOf, trace.ConnectionProblem(nil, "connection problem"))
	case <-ctx.Done():
		c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
	}

	for i := 0; i < testCase.retries; i++ {
		clock.BlockUntil(1)
		clock.Advance(scanPeriod + time.Second)

		// wait for upload success
		select {
		case event = <-eventsC:
			c.Assert(event.SessionID, check.Equals, sid)
			if i == testCase.retries-1 {
				c.Assert(event.Error, check.IsNil)
			} else {
				c.Assert(event.Error, check.FitsTypeOf, trace.ConnectionProblem(nil, "connection problem"))
			}
		case <-ctx.Done():
			c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
		}
	}

	// read the upload and make sure the data is equal
	outEvents := readStream(ctx, c, event.UploadID, memUploader)

	fixtures.DeepCompareSlices(c, inEvents, outEvents)

	// perform additional checks for defined by test case
	test.verify(c, testCase)
}

// emitStream creates and sends the session stream
func emitStream(ctx context.Context, c *check.C, streamer events.Streamer, inEvents []events.AuditEvent) {
	sid := inEvents[0].(events.SessionMetadataGetter).GetSessionID()

	stream, err := streamer.CreateAuditStream(ctx, session.ID(sid))
	c.Assert(err, check.IsNil)
	for _, event := range inEvents {
		err := stream.EmitAuditEvent(ctx, event)
		c.Assert(err, check.IsNil)
	}
	err = stream.Complete(ctx)
	c.Assert(err, check.IsNil)
}

// readStream reads and decodes the audit stream from uploadID
func readStream(ctx context.Context, c *check.C, uploadID string, uploader *events.MemoryUploader) []events.AuditEvent {
	parts, err := uploader.GetParts(uploadID)
	c.Assert(err, check.IsNil)

	var outEvents []events.AuditEvent
	var reader *events.ProtoReader
	for i, part := range parts {
		if i == 0 {
			reader = events.NewProtoReader(bytes.NewReader(part))
		} else {
			err := reader.Reset(bytes.NewReader(part))
			c.Assert(err, check.IsNil)
		}
		out, err := reader.ReadAll(ctx)
		c.Assert(err, check.IsNil, check.Commentf("part crash %#v", part))
		outEvents = append(outEvents, out...)
	}
	return outEvents
}
