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

package app

import (
	"context"
	"testing"
	"time"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/auth"
	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/utils"
	"github.com/jonboulle/clockwork"
	"github.com/pborman/uuid"

	"gopkg.in/check.v1"
)

type Suite struct {
	clock      clockwork.Clock
	authServer *auth.TestAuthServer
	tlsServer  *auth.TestTLSServer
	authClient *auth.Client
	appServer  *Server
	app        services.App
}

var _ = check.Suite(&Suite{})

func TestApp(t *testing.T) { check.TestingT(t) }

func (s *Suite) SetUpSuite(c *check.C) {
	var err error

	utils.InitLoggerForTests(testing.Verbose())

	s.clock = clockwork.NewFakeClockAt(time.Now())

	// Create Auth Server.
	s.authServer, err = auth.NewTestAuthServer(auth.TestAuthServerConfig{
		ClusterName: "localhost",
		Dir:         c.MkDir(),
	})
	c.Assert(err, check.IsNil)
	s.tlsServer, err = s.authServer.NewTestTLSServer()
	c.Assert(err, check.IsNil)

	// Create a client with a machine role of RoleApp.
	s.authClient, err = s.tlsServer.NewClient(auth.TestBuiltin(teleport.RoleApp))
	c.Assert(err, check.IsNil)

	// Create a services.App that will be used in all tests.
	staticLabels := map[string]string{
		"bar": "baz",
	}
	dynamicLabels := map[string]services.CommandLabel{
		"qux": &services.CommandLabelV2{
			Period:  services.NewDuration(time.Millisecond),
			Command: []string{"expr", "1", "+", "3"}},
	}
	s.app, err = services.NewApp("foo", staticLabels, services.AppSpecV3{
		Protocol:     teleport.WebAppProtocol,
		InternalAddr: "127.0.0.1:8080",
		PublicAddr:   "foo.example.com",
		Commands:     services.LabelsToV2(dynamicLabels),
		HostUUID:     uuid.New(),
		Version:      teleport.Version,
	})
	c.Assert(err, check.IsNil)
}

func (s *Suite) TearDownSuite(c *check.C) {
	err := s.authClient.Close()
	c.Assert(err, check.IsNil)

	err = s.tlsServer.Close()
	c.Assert(err, check.IsNil)
}

func (s *Suite) SetUpTest(c *check.C) {
	var err error

	s.appServer, err = New(&Config{
		Clock:        s.clock,
		AccessPoint:  s.authClient,
		CloseContext: context.Background(),
		GetRotation:  testRotationGetter,
		App:          s.app,
	})
	c.Assert(err, check.IsNil)

	err = s.appServer.Start()
	c.Assert(err, check.IsNil)

	err = s.appServer.heartbeat.ForceSend(time.Second)
	c.Assert(err, check.IsNil)
}

func (s *Suite) TearDownTest(c *check.C) {
	err := s.appServer.Close()
	c.Assert(err, check.IsNil)
}

// TestStart makes sure after the server has started a correct services.App
// has been created.
func (s *Suite) TestStart(c *check.C) {
	// Fetch the services.App that the service heartbeat.
	app, err := s.authServer.AuthServer.GetApp(context.Background(), defaults.Namespace, "foo")
	c.Assert(err, check.IsNil)

	// Check that the services.App that was heartbeat is correct. For example,
	// check that the dynamic labels have been evaluated.
	c.Assert(app.GetName(), check.Equals, "foo")
	c.Assert(app.GetInternalAddr(), check.Equals, "127.0.0.1:8080")
	c.Assert(app.GetPublicAddr(), check.Equals, "foo.example.com")
	c.Assert(app.GetStaticLabels(), check.DeepEquals, map[string]string{
		"bar": "baz",
	})
	dynamicLabel, ok := app.GetCommandLabels()["qux"]
	c.Assert(ok, check.Equals, true)
	c.Assert(dynamicLabel.GetResult(), check.Equals, "4")

	// Check the expiry time is correct.
	c.Assert(s.clock.Now().Before(app.Expiry()), check.Equals, true)
	c.Assert(s.clock.Now().Add(2*defaults.ServerAnnounceTTL).After(app.Expiry()), check.Equals, true)
}

// TestWaitStop makes sure the server will block and unlock.
func (s *Suite) TestWaitStop(c *check.C) {
	// Make sure that wait will block while the server is running.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s.appServer.Wait()
		cancel()
	}()
	select {
	case <-ctx.Done():
		c.Fatalf("Wait failed to block.")
	case <-time.After(250 * time.Millisecond):
	}

	// Close should unblock wait.
	err := s.appServer.Close()
	c.Assert(err, check.IsNil)
	err = s.appServer.Wait()
	c.Assert(err, check.NotNil)
}

func testRotationGetter(role teleport.Role) (*services.Rotation, error) {
	return &services.Rotation{}, nil
}
