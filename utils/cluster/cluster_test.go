package cluster_test

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/utils/cluster"
)

func TestCluster(t *testing.T) {
	segments := map[int]cluster.SegConfig{
		-1: cluster.SegConfig{DbID: 1, ContentID: -1, Port: 5432, Hostname: "localhost", DataDir: "/data/gpseg-1"},
		0:  cluster.SegConfig{DbID: 2, ContentID: 0, Port: 20000, Hostname: "localhost", DataDir: "/data/gpseg0"},
		2:  cluster.SegConfig{DbID: 4, ContentID: 2, Port: 20002, Hostname: "localhost", DataDir: "/data/gpseg2"},
		3:  cluster.SegConfig{DbID: 5, ContentID: 3, Port: 20003, Hostname: "remotehost2", DataDir: "/data/gpseg3"},
	}
	master := segments[-1]

	cases := []struct {
		name     string
		segments []cluster.SegConfig
	}{
		{"single-host, single-segment", []cluster.SegConfig{master, segments[0]}},
		{"single-host, multi-segment", []cluster.SegConfig{master, segments[0], segments[2]}},
		{"multi-host, multi-segment", []cluster.SegConfig{master, segments[0], segments[3]}},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s cluster", c.name), func(t *testing.T) {
			cluster := cluster.NewCluster(c.segments)

			actualContents := cluster.GetContentList()

			var expectedContents []int
			for _, seg := range c.segments {
				expectedContents = append(expectedContents, seg.ContentID)
			}

			if !reflect.DeepEqual(actualContents, expectedContents) {
				t.Errorf("had contents %v, want %v", actualContents, expectedContents)
			}

			for _, expected := range c.segments {
				content := expected.ContentID

				actual := cluster.Segments[content]
				if actual != expected {
					t.Errorf("had segment[%d] = %+v, want %+v", content, actual, expected)
				}

				actualHost := cluster.GetHostForContent(content)
				if actualHost != expected.Hostname {
					t.Errorf("had hostname[%d] = %q, want %q", content, actualHost, expected.Hostname)
				}
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
