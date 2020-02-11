package cluster_test

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/utils/cluster"
)

func TestCluster(t *testing.T) {
	primaries := map[int]cluster.SegConfig{
		-1: {DbID: 1, ContentID: -1, Port: 5432, Hostname: "localhost", DataDir: "/data/gpseg-1"},
		0:  {DbID: 2, ContentID: 0, Port: 20000, Hostname: "localhost", DataDir: "/data/gpseg0"},
		2:  {DbID: 4, ContentID: 2, Port: 20002, Hostname: "localhost", DataDir: "/data/gpseg2"},
		3:  {DbID: 5, ContentID: 3, Port: 20003, Hostname: "remotehost2", DataDir: "/data/gpseg3"},
	}
	for content, seg := range primaries {
		seg.Role = cluster.PrimaryRole
		primaries[content] = seg
	}

	mirrors := map[int]cluster.SegConfig{
		-1: {DbID: 8, ContentID: -1, Port: 5433, Hostname: "localhost", DataDir: "/mirror/gpseg-1"},
		0:  {DbID: 3, ContentID: 0, Port: 20001, Hostname: "localhost", DataDir: "/mirror/gpseg0"},
		2:  {DbID: 6, ContentID: 2, Port: 20004, Hostname: "localhost", DataDir: "/mirror/gpseg2"},
		3:  {DbID: 7, ContentID: 3, Port: 20005, Hostname: "remotehost2", DataDir: "/mirror/gpseg3"},
	}
	for content, seg := range mirrors {
		seg.Role = cluster.MirrorRole
		mirrors[content] = seg
	}

	master := primaries[-1]
	standby := mirrors[-1]

	cases := []struct {
		name      string
		primaries []cluster.SegConfig
		mirrors   []cluster.SegConfig
	}{
		{"mirrorless single-host, single-segment", []cluster.SegConfig{master, primaries[0]}, nil},
		{"mirrorless single-host, multi-segment", []cluster.SegConfig{master, primaries[0], primaries[2]}, nil},
		{"mirrorless multi-host, multi-segment", []cluster.SegConfig{master, primaries[0], primaries[3]}, nil},
		{"single-host, single-segment",
			[]cluster.SegConfig{master, primaries[0]},
			[]cluster.SegConfig{mirrors[0]},
		},
		{"single-host, multi-segment",
			[]cluster.SegConfig{master, primaries[0], primaries[2]},
			[]cluster.SegConfig{mirrors[0], mirrors[2]},
		},
		{"multi-host, multi-segment",
			[]cluster.SegConfig{master, primaries[0], primaries[3]},
			[]cluster.SegConfig{mirrors[0], mirrors[3]},
		},
		{"multi-host, multi-segment with standby",
			[]cluster.SegConfig{master, primaries[0], primaries[3]},
			[]cluster.SegConfig{standby, mirrors[0], mirrors[3]},
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s cluster", c.name), func(t *testing.T) {
			segments := append(c.primaries, c.mirrors...)

			cluster, err := cluster.NewCluster(segments)
			if err != nil {
				t.Fatalf("returned error %+v", err)
			}

			actualContents := cluster.GetContentList()

			var expectedContents []int
			for _, p := range c.primaries {
				expectedContents = append(expectedContents, p.ContentID)
			}

			if !reflect.DeepEqual(actualContents, expectedContents) {
				t.Errorf("GetContentList() = %v, want %v", actualContents, expectedContents)
			}

			for _, expected := range c.primaries {
				content := expected.ContentID

				actual := cluster.Primaries[content]
				if actual != expected {
					t.Errorf("Primaries[%d] = %+v, want %+v", content, actual, expected)
				}

				host := cluster.GetHostForContent(content)
				if host != expected.Hostname {
					t.Errorf("GetHostForContent(%d) = %q, want %q", content, host, expected.Hostname)
				}

				port := cluster.GetPortForContent(content)
				if port != expected.Port {
					t.Errorf("GetPortForContent(%d) = %d, want %d", content, port, expected.Port)
				}

				dbid := cluster.GetDbidForContent(content)
				if dbid != expected.DbID {
					t.Errorf("GetDbidForContent(%d) = %d, want %d", content, dbid, expected.DbID)
				}

				datadir := cluster.GetDirForContent(content)
				if datadir != expected.DataDir {
					t.Errorf("GetDirForContent(%d) = %q, want %q", content, datadir, expected.DataDir)
				}
			}

			for _, expected := range c.mirrors {
				content := expected.ContentID

				actual := cluster.Mirrors[content]
				if actual != expected {
					t.Errorf("Mirrors[%d] = %+v, want %+v", content, actual, expected)
				}
			}
		})
	}

	errCases := []struct {
		name     string
		segments []cluster.SegConfig
	}{
		{"bad role", []cluster.SegConfig{
			{Role: "x"},
		}},
		{"mirror without primary", []cluster.SegConfig{
			{ContentID: 0, Role: "p"},
			{ContentID: 1, Role: "m"},
		}},
		{"duplicated primary contents", []cluster.SegConfig{
			{ContentID: 0, Role: "p"},
			{ContentID: 0, Role: "p"},
		}},
		{"duplicated mirror contents", []cluster.SegConfig{
			{ContentID: 0, Role: "p"},
			{ContentID: 0, Role: "m"},
			{ContentID: 0, Role: "m"},
		}},
	}

	for _, c := range errCases {
		t.Run(fmt.Sprintf("doesn't allow %s", c.name), func(t *testing.T) {
			_, err := cluster.NewCluster(c.segments)

			if !xerrors.Is(err, cluster.ErrInvalidSegments) {
				t.Errorf("returned error %#v, want %#v", err, cluster.ErrInvalidSegments)
			}
		})
	}
}

func TestGetSegmentConfiguration(t *testing.T) {
	testhelper.SetupTestLogger() // init gplog

	cases := []struct {
		name     string
		rows     [][]driver.Value
		expected []cluster.SegConfig
	}{{
		"single-host, single-segment",
		[][]driver.Value{
			{"0", "localhost", "/data/gpseg0"},
		},
		[]cluster.SegConfig{
			{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"},
		},
	}, {
		"single-host, multi-segment",
		[][]driver.Value{
			{"0", "localhost", "/data/gpseg0"},
			{"1", "localhost", "/data/gpseg1"},
		},
		[]cluster.SegConfig{
			{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"},
			{ContentID: 1, Hostname: "localhost", DataDir: "/data/gpseg1"},
		},
	}, {
		"multi-host, multi-segment",
		[][]driver.Value{
			{"0", "localhost", "/data/gpseg0"},
			{"1", "localhost", "/data/gpseg1"},
			{"2", "remotehost", "/data/gpseg2"},
		},
		[]cluster.SegConfig{
			{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"},
			{ContentID: 1, Hostname: "localhost", DataDir: "/data/gpseg1"},
			{ContentID: 2, Hostname: "remotehost", DataDir: "/data/gpseg2"},
		},
	}}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s cluster", c.name), func(t *testing.T) {
			// Set up the connection to return the expected rows.
			rows := sqlmock.NewRows([]string{"contentid", "hostname", "datadir"})
			for _, row := range c.rows {
				rows.AddRow(row...)
			}

			connection, mock := testhelper.CreateAndConnectMockDB(1)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(rows)
			defer func() {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("%v", err)
				}
			}()

			results, err := cluster.GetSegmentConfiguration(connection)
			if err != nil {
				t.Errorf("returned error %+v", err)
			}

			if !reflect.DeepEqual(results, c.expected) {
				t.Errorf("got configuration %+v, want %+v", results, c.expected)
			}
		})
	}
}
