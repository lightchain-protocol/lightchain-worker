// Package buildinfo reports which build of the worker is running, so an
// operator (and anyone helping them) can tell which release a host is on.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Version is the release tag, stamped at build time:
//
//	go build -ldflags "-X github.com/lightchain/worker/internal/buildinfo.Version=v1.2.3"
//
// make release and the Dockerfile set it. A plain go build leaves "dev".
var Version = "dev"

// Info describes the running build.
type Info struct {
	Version  string // release tag, or "dev"
	Revision string // VCS commit, when the Go toolchain recorded one
	Modified bool   // the build's working tree had uncommitted changes
	Go       string // Go toolchain version
	Platform string // GOOS/GOARCH
}

// Get returns the running build's Info.
func Get() Info {
	return fromBuildInfo(Version, debug.ReadBuildInfo)
}

// String is Get formatted for one line of output.
func String() string {
	return Get().String()
}

func fromBuildInfo(version string, read func() (*debug.BuildInfo, bool)) Info {
	info := Info{
		Version:  version,
		Go:       runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
	}
	if version == "" {
		info.Version = "dev"
	}
	bi, ok := read()
	if !ok || bi == nil {
		return info
	}
	if bi.GoVersion != "" {
		info.Go = bi.GoVersion
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			info.Revision = s.Value
		case "vcs.modified":
			info.Modified = s.Value == "true"
		}
	}
	return info
}

// String formats the build as, for example,
// "lightchain-worker v1.2.3 (commit 1a2b3c4d5e6f, go1.26.0, linux/amd64)".
// The commit is left out when the build did not record one, as in the
// Docker image, which builds without the .git directory.
func (i Info) String() string {
	details := ""
	if i.Revision != "" {
		rev := i.Revision
		if len(rev) > 12 {
			rev = rev[:12]
		}
		if i.Modified {
			rev += "-dirty"
		}
		details = "commit " + rev + ", "
	}
	return fmt.Sprintf("lightchain-worker %s (%s%s, %s)", i.Version, details, i.Go, i.Platform)
}
