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

package srv

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/trace"
	"github.com/sirupsen/logrus"
)

type DynamicLabelsConfig struct {
	// Labels is the list of dynamic labels to update.
	Labels services.CommandLabels

	// CloseContext is used to signal when to stop periodic functions.
	CloseContext context.Context

	// ComponentName is the name of the component keeping dynamic labels updated.
	ComponentName string
}

func (c *DynamicLabelsConfig) CheckAndSetDefaults() error {
	// Loop over all labels and make sure the key name is valid and the interval
	// is valid as well. If the interval is not valid, update the value.
	labels := c.Labels.Clone()
	for name, label := range labels {
		if !services.IsValidLabelKey(name) {
			return trace.BadParameter("invalid label key: %q", name)
		}

		if label.GetPeriod() < time.Second {
			label.SetPeriod(time.Second)
			labels[name] = label
			logrus.Warningf("Label period can't be less that 1 second. Period for label %q was set to 1 second.", name)
		}
	}
	c.Labels = labels

	return nil
}

type DynamicLabels struct {
	*DynamicLabelsConfig

	mu     sync.Mutex
	log    *logrus.Entry
	labels map[string]services.CommandLabel
}

func NewDynamicLabels(config *DynamicLabelsConfig) (*DynamicLabels, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}

	dynamicLabels := &DynamicLabels{
		DynamicLabelsConfig: config,
		log: logrus.WithFields(logrus.Fields{
			trace.Component: config.ComponentName,
		}),
	}

	return dynamicLabels, nil
}

// Get returns the list of updated dynamic labels.
func (l *DynamicLabels) Get() map[string]services.CommandLabel {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make(map[string]services.CommandLabel, len(l.Labels))
	for name, label := range l.Labels {
		out[name] = label.Clone()
	}

	return out
}

// Sync will synchronously update dynamic labels. Used in tests.
func (l *DynamicLabels) Sync() {
	for name, label := range l.Get() {
		l.updateLabel(name, label)
	}
}

// Async will run a loop that asynchronous keeps the dynamic labels updated.
func (l *DynamicLabels) Async() {
	for name, label := range l.Get() {
		go l.periodicUpdateLabel(name, label.Clone())
	}
}

func (l *DynamicLabels) periodicUpdateLabel(name string, label services.CommandLabel) {
	ticker := time.NewTicker(label.GetPeriod())
	defer ticker.Stop()

	for {
		l.updateLabel(name, label.Clone())
		select {
		case <-ticker.C:
		case <-l.CloseContext.Done():
			return
		}
	}
}

func (l *DynamicLabels) updateLabel(name string, label services.CommandLabel) {
	out, err := exec.Command(label.GetCommand()[0], label.GetCommand()[1:]...).Output()
	if err != nil {
		l.log.Errorf(err.Error())
		label.SetResult(err.Error() + " output: " + string(out))
	} else {
		label.SetResult(strings.TrimSpace(string(out)))
	}

	// Perform the actual label update under a lock.
	l.setCommandLabel(name, label)
}

func (l *DynamicLabels) setCommandLabel(name string, value services.CommandLabel) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Labels[name] = value
}
