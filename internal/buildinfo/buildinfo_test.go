package buildinfo

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func TestFromBuildInfo(t *testing.T) {
	t.Parallel()

	platform := runtime.GOOS + "/" + runtime.GOARCH

	tests := []struct {
		name    string
		version string
		read    func() (*debug.BuildInfo, bool)
		want    Info
		str     string
	}{
		{
			name:    "release with commit",
			version: "v1.2.3",
			read: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{
					GoVersion: "go1.26.0",
					Settings: []debug.BuildSetting{
						{Key: "vcs.revision", Value: "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b"},
						{Key: "vcs.modified", Value: "false"},
					},
				}, true
			},
			want: Info{Version: "v1.2.3", Revision: "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b", Go: "go1.26.0", Platform: platform},
			str:  "lightchain-worker v1.2.3 (commit 1a2b3c4d5e6f, go1.26.0, " + platform + ")",
		},
		{
			name:    "local build with uncommitted changes",
			version: "dev",
			read: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{
					GoVersion: "go1.26.0",
					Settings: []debug.BuildSetting{
						{Key: "vcs.revision", Value: "abc123"},
						{Key: "vcs.modified", Value: "true"},
					},
				}, true
			},
			want: Info{Version: "dev", Revision: "abc123", Modified: true, Go: "go1.26.0", Platform: platform},
			str:  "lightchain-worker dev (commit abc123-dirty, go1.26.0, " + platform + ")",
		},
		{
			name:    "no VCS data, as in the Docker image",
			version: "v1.2.3",
			read: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{GoVersion: "go1.26.0"}, true
			},
			want: Info{Version: "v1.2.3", Go: "go1.26.0", Platform: platform},
			str:  "lightchain-worker v1.2.3 (go1.26.0, " + platform + ")",
		},
		{
			name:    "build info unavailable, empty version",
			version: "",
			read:    func() (*debug.BuildInfo, bool) { return nil, false },
			want:    Info{Version: "dev", Go: runtime.Version(), Platform: platform},
			str:     "lightchain-worker dev (" + runtime.Version() + ", " + platform + ")",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fromBuildInfo(tc.version, tc.read)
			if got != tc.want {
				t.Errorf("fromBuildInfo() = %+v, want %+v", got, tc.want)
			}
			if s := got.String(); s != tc.str {
				t.Errorf("String() = %q, want %q", s, tc.str)
			}
		})
	}
}

func TestGetDefaultsToDev(t *testing.T) {
	t.Parallel()

	// Tests are built without the release ldflags, so Version keeps its default.
	if got := Get().Version; got != "dev" {
		t.Errorf("Get().Version = %q, want %q", got, "dev")
	}
}
