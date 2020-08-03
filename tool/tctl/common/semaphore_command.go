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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gravitational/kingpin"
	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/asciitable"
	"github.com/gravitational/teleport/lib/auth"
	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/service"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/trace"
)

// SemaphoreCommand implements basic semaphore operations.
type SemaphoreCommand struct {
	config *service.Config

	username string
	leaseID  string

	format string

	force   bool
	verbose bool

	scList   *kingpin.CmdClause
	scDelete *kingpin.CmdClause
}

// Initialize allows SemaphoreCommand to plug itself into the CLI parser
func (c *SemaphoreCommand) Initialize(app *kingpin.Application, config *service.Config) {
	c.config = config

	sc := app.Command("sessctl", "Session-control introspection & management").Alias("session-control")

	c.scList = sc.Command("ls", "List active session-control leases").Alias("list")
	c.scList.Flag("format", "Output format, 'text' or 'json'").Default(teleport.Text).StringVar(&c.format)
	c.scList.Flag("verbose", "Output verbose lease details").Short('v').BoolVar(&c.verbose)
	c.scList.Flag("user", "Filter output by username").StringVar(&c.username)

	c.scDelete = sc.Command("rm", "Delete active session-control leases").Alias("delete").Hidden()
	c.scDelete.Flag("force", "Permits unconstrained deletions").Short('f').BoolVar(&c.force)
	c.scDelete.Flag("user", "Username of target").StringVar(&c.username)
	c.scDelete.Flag("lease", "Lease ID of target").StringVar(&c.leaseID)
}

// TryRun takes the CLI command as an argument (like "access-request list") and executes it.
func (c *SemaphoreCommand) TryRun(cmd string, client auth.ClientI) (match bool, err error) {
	switch cmd {
	case c.scList.FullCommand():
		err = c.List(client)
	case c.scDelete.FullCommand():
		err = c.Delete(client)
	default:
		return false, nil
	}
	return true, trace.Wrap(err)
}

// List lists all matching semaphores.
func (c *SemaphoreCommand) List(client auth.ClientI) error {
	sems, err := client.GetSemaphores(context.TODO(), services.SemaphoreFilter{
		SemaphoreName: c.username,
		SemaphoreKind: services.SemaphoreKindSessionControl,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(c.PrintSemaphores(client, sems, c.format, c.verbose))
}

// Delete deletes all matching semaphores.
func (c *SemaphoreCommand) Delete(client auth.ClientI) error {
	if c.leaseID != "" {
		if c.username == "" {
			return trace.BadParameter("cannod resolve lease %q without username", c.leaseID)
		}
		return trace.Wrap(client.CancelSemaphoreLease(context.TODO(), services.SemaphoreLease{
			SemaphoreKind: services.SemaphoreKindSessionControl,
			SemaphoreName: c.username,
			LeaseID:       c.leaseID,
			Expires:       time.Now().UTC().Add(time.Minute),
		}))
	}
	if !c.force && c.username == "" {
		return trace.BadParameter("user name must be specified; use -f/--force to override (dangerous)")
	}
	return trace.Wrap(client.DeleteSemaphores(context.TODO(), services.SemaphoreFilter{
		SemaphoreName: c.username,
		SemaphoreKind: services.SemaphoreKindSessionControl,
	}))
}

func (c *SemaphoreCommand) PrintSemaphores(client auth.ClientI, sems []services.Semaphore, format string, verbose bool) error {
	switch {
	case format == teleport.Text && !verbose:
		// resolve node hostnames and print "pretty" table.
		nodes, err := client.GetNodes(defaults.Namespace)
		if err != nil {
			return trace.Wrap(err)
		}
		table := asciitable.MakeTable([]string{"User", "LeaseID", "Host"})
		for _, sem := range sems {
			for _, ref := range sem.LeaseRefs() {
				nodeName := ref.Holder
				for _, node := range nodes {
					if node.GetName() == ref.Holder {
						if node.GetHostname() != "" {
							nodeName = node.GetHostname()
						}
						break
					}
				}
				table.AddRow([]string{
					sem.GetName(),
					ref.LeaseID,
					nodeName,
				})
			}
		}
		_, err = table.AsBuffer().WriteTo(os.Stdout)
		return trace.Wrap(err)
	case format == teleport.Text:
		// print a verbose table containing raw semaphore lease data
		table := asciitable.MakeTable([]string{"User", "LeaseID", "NodeID", "Expires"})
		for _, sem := range sems {
			for _, ref := range sem.LeaseRefs() {
				table.AddRow([]string{
					sem.GetName(),
					ref.LeaseID,
					ref.Holder,
					ref.Expires.Format(time.RFC822),
				})
			}
		}
		_, err := table.AsBuffer().WriteTo(os.Stdout)
		return trace.Wrap(err)
	default:
		out, err := json.MarshalIndent(sems, "", "  ")
		if err != nil {
			return trace.Wrap(err, "failed to marshal semaphores")
		}
		fmt.Printf("%s\n", out)
	}
	return nil
}
