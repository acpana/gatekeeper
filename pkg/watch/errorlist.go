/*

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

package watch

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// errorList is an error that aggregates multiple gvkErrs.
type errorList struct {
	errs        []gvkErr
	isUniversal bool
}

type WatchesError interface {
	// returns gvks for which we had watch errors
	FailingGVKs() []schema.GroupVersionKind
	// returns true if this error is not specific to the failing gvks
	IsUniversal() bool
	Error() string
}

// a gvk annotated err.
type gvkErr struct {
	err error
	gvk schema.GroupVersionKind
}

func (w gvkErr) String() string {
	return w.Error()
}

func (w gvkErr) Error() string {
	return fmt.Sprintf("error for gvk: %s: %s", w.gvk, w.err.Error())
}

func (e errorList) String() string {
	return e.Error()
}

func (e errorList) Error() string {
	var builder strings.Builder
	for i, err := range e.errs {
		if i > 0 {
			builder.WriteRune('\n')
		}
		builder.WriteString(err.Error())
	}
	return builder.String()
}

func (e errorList) FailingGVKs() []schema.GroupVersionKind {
	gvks := make([]schema.GroupVersionKind, len(e.errs))
	for _, err := range e.errs {
		gvks = append(gvks, err.gvk)
	}

	return gvks
}

func (e errorList) IsUniversal() bool {
	return e.isUniversal
}
